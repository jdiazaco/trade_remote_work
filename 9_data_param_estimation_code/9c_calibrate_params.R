# setup -------------------------------------------------------------------
if(exists('base_env')){rm(list= setdiff(ls(), base_env))}else{rm(list = rm(list = ls()))}; gc();
## set working directory dynamically 
{
  library(dplyr)
  root = case_when(
    ## AZM running locally and not testing if it will work CASD 
    grepl("/Users/amagnuson",getwd()) & !grepl('4) exports-imports',getwd()) ~ "/Users/amagnuson/Library/CloudStorage/GoogleDrive-amagnuson@g.harvard.edu/My Drive/Grad School/8) all projects/Trade/Big Data",
    
    ## update as makes sense for CASD / your own use 
    T ~ "C:/Users/Public/Documents/Big data Project")
  setwd(root)
}

## import helper functions 
source('2) code/0_set_parameter_values.R')
# helper functions --------------------------------------------------------
map_params = function(eta, theta_max = .999, alpha_max = .999){
 names(eta) = eta_names
   list(
    mu_a    = softplus(eta[["mu_a_raw"]]),
    phi_d_tilde   = softplus(eta[["phi_d_tilde_raw"]]),
    alpha_1 = alpha_max *logistic(eta[["alpha_1_raw"]]),
    alpha_2 = alpha_max*logistic(eta[["alpha_2_raw"]]),
    sigma_a = softplus(eta[["sigma_a_raw"]]),
    Q     = softplus(eta[["Q_raw"]]),
    theta = theta_max * logistic(eta[["theta_raw"]]),
    f0      = logistic(eta[["f0_raw"]])   
  )
}

eval_g_ineq <- function(x){
  params=  calc_log_likelihood(x, 'diagnostic')[['output_params']]
  g = params$A_bar_nonneg_qual - params$A_bar_calc
  return(g)  
}

# import and clean data -----------------------------------------------------------------------
creating_dta = F
if (creating_dta){
min_streak_length = 8
min_age_for_likelihood = 3
vars_to_windsorize = c('L_t', 'Ex_t')

dta = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>% 
  .[first_forecast == T & fiscal_year %in% us_year_range] %>%  
  .[!log_forecast_out_of_range== T, FE_t := (realized_value - forecast)*1e-6] %>%
  unbalanced_lag('ibes_ticker', 'fiscal_year', 'forecast', 1) %>% # NB comp_data_lag1 already in data 
  .[, `:=`(lacks_needed_info = is.na(comp_data_lag1) | is.na(forecast_lag1),
           L_t = comp_data*1e-6,
           Ex_t = forecast*1e-6,
           x_t = realized_value*1e-6)] %>% 
  setorder(ibes_ticker, fiscal_year) %>%
  .[,streak_id := cumsum(lacks_needed_info)] %>% 
  .[, .(ibes_ticker, streak_id, fiscal_year,FE_t,x_t, L_t, Ex_t)] %>% 
  .[,streak_length :=.N, by = streak_id] %>%
  .[streak_length >= min_streak_length] %>% 
  .[, idx := .I] %>% 
  .[, `:=`(age = seq_len(.N) - 1L, prev_row_idx = shift(idx, 1)), by = streak_id] %>% 
  .[,  x_bar := NA_median(x_t), by = ibes_ticker] %>% 
  .[, likelihood_eligible := age>=min_age_for_likelihood & !is.na(FE_t)] %>% 
  .[, (vars_to_windsorize) := lapply(.SD, function(x) winsorize_relative(x, x_bar,.05, .95, 'ratio')), .SDcols = vars_to_windsorize] %>% 
  .[, .(likelihood_eligible,ibes_ticker, streak_id, fiscal_year,age, idx, prev_row_idx, x_bar,FE_t, L_t, Ex_t)]
write_parquet(dta, "1) data/11_parameter_calibration/clean/4_calibration_input.parquet")
}
dta = import_file("1) data/11_parameter_calibration/clean/4_calibration_input.parquet")


## define key vectors 
ages <- sort(unique(dta$age))
rows_by_age <- lapply(ages, function(a) dta[age == a, idx])
prev_idx <- dta$prev_row_idx
n = nrow(dta)
scale_L = median(dta$L_t, na.rm=TRUE)
scale_E = median(dta$Ex_t, na.rm=TRUE)
# we use scaled versions of L_t and Ex_t 
# which means what we estimate is tilde_phi_d = phi_d*scale_L^alpha_1*scale_E^alpha_2
L_t = dta$L_t / scale_L 
Ex_t = dta$Ex_t / scale_E
FE_t = dta$FE_t
idx_FE = dta$likelihood_eligible
x_bar = dta$x_bar
EPS    <- 1e-10

eta0 = list(mu_a_raw = softplus_inv(1.2), 
           phi_d_tilde_raw = softplus_inv(.1*scale_L^.5*scale_E^.1),
           alpha_1_raw = qlogis(.5),
           alpha_2_raw = qlogis(.1), 
           sigma_a_raw = softplus_inv(1.1777),
           Q_raw = softplus_inv(.66),
           theta_raw = qlogis(.45),
           f0_raw = qlogis(.5))
eta_names = names(eta0)


# log_likelihood calc  -------------------------------------------------------
library('MASS')
calc_log_likelihood = function(eta, mode = c('calc', 'diagnostic')){
  mode = match.arg(mode)
  params = map_params(eta)
  
  ## ACTUAL FUNCTION 
  for (name in names(params)) assign(name, params[[name]], envir = environment())
  sig2_a <- sigma_a^2
  th2 = theta^2
  Sigma_ub <- Q / (1 - th2) #theta is bounded (0,1)
  #A_bar <- mu_a * (Sigma_ub + sig2_a) + softplus(kappa)
  
  #Initialize Sigma_tt --> set all firms initial uncertainty to .5*Sigma_ub
  Sigma_t          = numeric(n)
  Sigma_tplus1_t   = numeric(n)
  n_tplus1         = numeric(n)
  
  # initialization at age 0:
  idx                 = rows_by_age[[1]]
  Sigma_t[idx]        = f0 * Sigma_ub
  Sigma_tplus1_t[idx] = Sigma_t[idx]*th2 + Q
  n_tplus1[idx]       = phi_d_tilde * L_t[idx]^alpha_1 * Ex_t[idx]^alpha_2
  Sigma_tplus1_t[idx] = Sigma_tplus1_t[idx] /  pmax(1 + Sigma_tplus1_t[idx]*n_tplus1[idx], EPS)
  
  for (k in ages[-1]) {
    idx = rows_by_age[[k+1]]
    Sigma_t[idx] = pmax(Sigma_tplus1_t[prev_idx[idx]], 0)
    if (k != max(ages)) {
      Sigma_tplus1_t[idx] = Sigma_t[idx]*th2 + Q
      n_tplus1[idx]       = phi_d_tilde * L_t[idx]^alpha_1 * Ex_t[idx]^alpha_2
      Sigma_tplus1_t[idx] = Sigma_tplus1_t[idx] /  pmax(1 + Sigma_tplus1_t[idx]*n_tplus1[idx], EPS)
    }
  }
  
  ## perform the likelihood 
  pi_mix <- 0.01   # amount of guassian in estimation. Stops us falling off cliffs 
  s2   <- Sigma_t[idx_FE] + sig2_a
  S    <- x_bar[idx_FE] * mu_a * s2         # c_t
  U    <- 1 - FE_t[idx_FE] / S              # Y_t ~ χ^2_1
  bad <- !is.finite(U) | U <= 0
  log_exact <- dchisq(U, df=1, log=TRUE) - log(pmax(S, EPS)); log_exact[bad] <- -Inf
  log_norm  <- dnorm(FE_t[idx_FE], mean=0, sd=sqrt(2)*S, log=TRUE)
  
  log_a <- log1p(-pi_mix) + log_exact
  log_b <- log(pi_mix)    + log_norm
  ll    <- matrixStats::rowLogSumExps(cbind(log_a, log_b))  
  ll_sum <- sum(ll, na.rm = TRUE)
  nll = -ll_sum
  if (mode == 'calc'){ return(nll)}
  if (mode == 'diagnostic'){
    output = dta[,.(ibes_ticker,x_bar, fiscal_year,age, FE_t,L_t,Ex_t, Sigma_t)] %>% 
      .[idx_FE == T, ll := ll]
    
    params$Sigma_ub = Sigma_ub
    params$phi_d = phi_d_tilde / (scale_L^alpha_1*scale_E^alpha_2)
    
    ### HANDLE A_BAR 
    ## A_bar_calculated relies on the formula 
    ## E[x_t] = x_bar*(A_bar -mu_a(Sigma_t + sigma_a^2)) --> we invert and take the average
    ## A_bar_nonneg_qual is the minimum value of A_bar such that A_bar-mu_a(Sigma_ub + sigma_a^2) > 0 (we always have positive quality)
    params$A_bar_calc =  NA_mean(dta$Ex_t[idx_FE] / dta$x_bar[idx_FE] + mu_a*(output$Sigma_t[idx_FE] + sig2_a))
    params$A_bar_nonneg_qual = mu_a*(Sigma_ub+ sig2_a)
    
    return(list(output_df = output, output_params = params))
  }
}                               
obj <- function(eta) calc_log_likelihood(eta, 'calc')
ll_total = function(eta_num){-calc_log_likelihood(eta_num, 'calc')}
ll_vec = function(eta_num){calc_log_likelihood(eta_num, 'diagnostic')$output_df[!is.na(ll)]$ll}
theta_from_eta = function(eta_num){as.numeric(calc_log_likelihood(eta_num, 'diagnostic')$output_params)}
invert <- function(M) {
  co <- try(solve(M), silent = TRUE);
  if (inherits(co, "try-error")) MASS::ginv(M) else co}


## 0) Perform the maximum likelihood optimization 
res <- nloptr::nloptr(x0 = as.numeric(eta0), eval_f = function(x) list(objective = obj(x), gradient = numeric(length(x))),
  eval_g_ineq = eval_g_ineq,opts = list(algorithm="NLOPT_LN_COBYLA", maxeval=5000, xtol_rel=1e-8))

eta_hat = res$solution
Theta_hat = calc_log_likelihood(res$solution, 'diagnostic')$output_params


make_start <- function(eta0, jitter = 0.5, bounds = NULL) {
  x <- as.numeric(eta0)
  # logit raws: alpha_1_raw, alpha_2_raw, theta_raw, f0_raw
  # softplus raws: mu_a_raw, phi_d_tilde_raw, sigma_a_raw, Q_raw
  names(x) <- names(eta0)
  
  # default raw bounds (loose but finite)
  if (is.null(bounds)) bounds <- list(
    mu_a_raw         = c( softplus_inv(1e-3), softplus_inv(5) ),
    phi_d_tilde_raw  = c( softplus_inv(1e-4), softplus_inv(5) ),
    alpha_1_raw      = c( qlogis(.02),      qlogis(.98) ),
    alpha_2_raw      = c( qlogis(.02),      qlogis(.98)),
    sigma_a_raw      = c( softplus_inv(1e-4), softplus_inv(2) ),
    Q_raw            = c( softplus_inv(1e-4), softplus_inv(5) ),
    theta_raw        = c( qlogis(1e-4),      qlogis(1-1e-4) ),
    f0_raw           = c( qlogis(1e-4),      qlogis(1-1e-4) )
  )
  
  # jitter around eta0 within bounds
  for (nm in names(x)) {
    lo <- bounds[[nm]][1]; hi <- bounds[[nm]][2]
    span <- (hi - lo)
    prop_jit <- runif(1, -jitter, jitter)             # ±jitter fraction of span
    cand <- x[[nm]] + prop_jit * span
    x[[nm]] <- min(hi, max(lo, cand))
  }
  x
}
fit_from_start <- function(x0_raw) {
  nloptr::nloptr(
    x0 = x0_raw,
    eval_f = function(x) list(objective = obj(x), gradient = numeric(length(x))),
    eval_g_ineq = eval_g_ineq,
    opts = list(algorithm="NLOPT_LN_COBYLA", maxeval=5000, xtol_rel=1e-8)
  )
}
set.seed(42)
K <- 20  # start with 10–20; bump if you see frequent “ties”
starts <- replicate(K, make_start(eta0, jitter=0.8), simplify = FALSE)
fits <- lapply(starts, fit_from_start)
tol <- 1e-6
feasible <- vapply(fits, function(fr) {
  gi <- try(eval_g_ineq(fr$solution), silent=TRUE)
  is.numeric(gi) && all(gi <= tol)
}, logical(1))
objvals <- vapply(fits, function(fr) fr$objective, numeric(1))
pick <- if (any(feasible)) which.min(replace(objvals, !feasible, Inf)) else which.min(objvals)
best_fit <- fits[[pick]]

eta_hat = best_fit$solution


## 1) Observed information (Hessian) at eta_hat
H = try(hessian(ll_total, eta_hat), silent = TRUE)
V_eta_CR <- invert(-H)  # correctly specified covariance (CR = "classical/regular")

## 2) Sandwich (cluster-robust) covariance on eta
  # Scores: Jacobian of per-obs ll wrt eta gives an (n x k) matrix
  J <- jacobian(ll_vec, eta_hat)  # may take some seconds
  cluster_id <- calc_log_likelihood(eta_hat, 'diagnostic')$output_df[!is.na(ll)]$ibes_ticker
  cl_levels <- unique(cluster_id)
  k <- ncol(J)
  S <- matrix(0, k, k)
  
  for (g in cl_levels) {
    rows_g <- which(cluster_id == g)
    s_g <- colSums(J[rows_g, , drop = FALSE])        # (1 x k)
    S <- S + tcrossprod(s_g)                          # add s_g s_g'
  }
  
  V_eta_SW <- V_eta_CR %*% S %*% V_eta_CR
  
  se_eta_CR <- sqrt(diag(V_eta_CR))
  se_eta_SW <- sqrt(diag(V_eta_SW))
  
  ## 3) Delta-method to structural parameters Theta (incl. phi_d)
  G <- jacobian(theta_from_eta, eta_hat)  # (k_theta x k_eta)
  V_theta_CR <- G %*% V_eta_CR %*% t(G)
  V_theta_SW <- G %*% V_eta_SW %*% t(G)
  
  se_theta_CR <- sqrt(diag(V_theta_CR))
  se_theta_SW <- sqrt(diag(V_theta_SW))
  
  
Theta_hat = calc_log_likelihood(eta_hat, 'diagnostic')$output_params
se_table <- data.frame(
  param = names(Theta_hat),
  est   = as.numeric(Theta_hat),
  se_classical = as.numeric(se_theta_CR),
  se_clustered = as.numeric(se_theta_SW),
  row.names = NULL
)  


  
output = calc_log_likelihood(res$solution, 'diagnostic')
output_params = output$output_params
output_params = data.frame(variable = names(output_params), value = unlist(output_params))
fwrite(output_params,'1) data/11_parameter_calibration/clean/parameter_output.csv')




