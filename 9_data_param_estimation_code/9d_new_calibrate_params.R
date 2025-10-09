# setup -------------------------------------------------------------------
if(exists('base_env')){rm(list= setdiff(ls(), base_env))}else{ rm(list = ls())}; gc();
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
    alpha_1 = alpha_max *logistic(eta[["alpha_1_raw"]]),
    alpha_2 = alpha_max*logistic(eta[["alpha_2_raw"]]),
    Q     = 1e-4 + softplus(eta[["Q_raw"]]),
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
  vars_to_windsorize = c('L_t', 'Ex_t', 'FE_t')
  top_data_cutoff = .9
  
  dta = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>% 
    .[first_forecast == T & fiscal_year %in% us_year_range] %>%  
    .[!log_forecast_out_of_range== T, FE_t := (realized_value - forecast)*1e-6] %>%
    unbalanced_lag('ibes_ticker', 'fiscal_year', 'forecast', 1) %>% 
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
  
  best_case_sample = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>%
    .[ first_forecast == T & !is.na(comp_data) & fiscal_year %in% us_year_range] %>% merge(
      .[!is.na(naics_2d)] %>%
        .[, .(surprise = median(abs_firm_log_forecast_error, na.rm=TRUE)), by=.(naics_2d, fiscal_year)] %>%
        .[, norm_surprise := surprise / median(surprise, na.rm=TRUE), by=naics_2d] %>%
        .[, rank := frank(norm_surprise, ties.method="average"), by=naics_2d] %>% 
        .[,`:=`(tranquil = rank <= floor(0.20*.N), shock = rank > ceiling(.8*.N)), by=naics_2d] %>%
        .[, .(naics_2d, tranquil, shock, fiscal_year)], all.x = T ) %>% 
    .[, data_cutoff := quantile(comp_data, top_data_cutoff), by = fiscal_year] %>%
    .[, tranquil_big_sample := tranquil == T & comp_data >= data_cutoff & is.finite(abs_firm_log_forecast_error)] %>% 
    .[, .(ibes_ticker, fiscal_year, tranquil_big_sample)] 
  
  dta = merge(dta, best_case_sample, all.x = T) %>% .[,tranquil_big_sample := case_when(
    is.na(likelihood_eligible) | likelihood_eligible == F ~ F,
    is.na(tranquil_big_sample) | tranquil_big_sample == F ~ F,
    T ~ T)] 
  
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
L_t = dta$L_t / scale_L 
Ex_t = dta$Ex_t / scale_E
FE_t = dta$FE_t
idx_FE = dta$likelihood_eligible
idx_stable = dta$tranquil_big_sample
x_bar = dta$x_bar
EPS    <- 1e-10

eta0 = list(alpha_1_raw = qlogis(.5),
            alpha_2_raw = qlogis(.1), 
            Q_raw = softplus_inv(.66),
            theta_raw = qlogis(.45),
            f0_raw = qlogis(.5))
eta_names = names(eta0)


bounds <- list(
  alpha_1_raw = c(qlogis(.02), qlogis(.98)),
  alpha_2_raw = c(qlogis(.02), qlogis(.98)),
  Q_raw       = c(softplus_inv(1e-4), softplus_inv(5)),
  theta_raw   = c(qlogis(1e-4), qlogis(1-1e-4)),
  f0_raw      = c(qlogis(1e-4), qlogis(1-1e-4))
)
lower <- sapply(bounds, `[`, 1); upper <- sapply(bounds, `[`, 2)
# perform likelihood estimation -------------------------------------------
library('MASS'); library('numDeriv')
calc_log_likelihood = function(eta, mode = c('calc', 'diagnostic')){
  softplus_inner <- function(x, beta=75) (1/beta)*log1p(exp(beta*x))
  soft_floor_inner <- function(x, eps=1e-8, beta=75) eps + softplus_inner(x-eps, beta)
  smooth_max <- function(a, b, tau) 0.5 * (a + b + sqrt((a - b)^2 + tau^2))
  
  mode = match.arg(mode)
  params = map_params(eta)
  
  ## ACTUAL FUNCTION 
  for (name in names(params)) assign(name, params[[name]], envir = environment())
  mu_a = 1
  phi_d_tilde =  scale_L^alpha_1*scale_E^alpha_2
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
  Sigma_tplus1_t[idx] = Sigma_tplus1_t[idx] / soft_floor_inner(1 + Sigma_tplus1_t[idx]*n_tplus1[idx], eps=1e-12)
  
  ## roll the kalman filter forward 
  for (k in ages[-1]) {
    idx = rows_by_age[[k+1]]
    Sigma_t[idx] = softplus_inner(Sigma_tplus1_t[prev_idx[idx]], beta=75)
    if (k != max(ages)) {
      Sigma_tplus1_t[idx] = Sigma_t[idx]*th2 + Q
      n_tplus1[idx]       = phi_d_tilde * L_t[idx]^alpha_1 * Ex_t[idx]^alpha_2
      Sigma_tplus1_t[idx] = Sigma_tplus1_t[idx] / soft_floor_inner(1 + Sigma_tplus1_t[idx]*n_tplus1[idx], eps=1e-12)
    }
  }
  
  ## perform the likelihood 
  pi_mix <- 1   # amount of guassian in estimation. Stops us falling off cliffs 
  s2   <- Sigma_t[idx_FE]
  S    <- x_bar[idx_FE] * mu_a * s2                        # could be tiny/±
 
  u_raw <- 1 - FE_t[idx_FE] / S
  u_min <- 1e-4   # if NLL still misbehaves, try 1e-3; if too blunt, 1e-5
  tau_u <- 1e-4
  U     <- u_raw#smooth_max(u_raw, u_min, tau_u)
  
  # 3) components (no hard pmax anywhere; S_pos used consistently)
  log_exact <- dchisq(U, df=1, log=TRUE) - log(S)
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
    params$FE_obs = sum(idx_FE)
    ### HANDLE A_BAR 
    ## A_bar_calculated relies on the formula 
    ## E[x_t] = x_bar*(A_bar -mu_a(Sigma_t + sigma_a^2)) --> we invert and take the average
    ## A_bar_nonneg_qual is the minimum value of A_bar such that A_bar-mu_a(Sigma_ub + sigma_a^2) > 0 (we always have positive quality)
    params$A_bar_calc =  NA_mean(dta$Ex_t[idx_FE] / dta$x_bar[idx_FE] + mu_a*(output$Sigma_t[idx_FE]))
    params$A_bar_nonneg_qual = mu_a*(Sigma_ub)
    return(list(output_df = output, output_params = params))
  }
}  
k_initial_runs = function(K){
  obj <- function(eta) calc_log_likelihood(eta, 'calc')
  make_start <- function(eta0, jitter = 0.5, bounds = NULL) {
    x <- as.numeric(eta0)
    # logit raws: alpha_1_raw, alpha_2_raw, theta_raw, f0_raw
    # softplus raws: mu_a_raw, phi_d_tilde_raw, sigma_a_raw, Q_raw
    names(x) <- names(eta0)
    
    # default raw bounds (loose but finite)
    if (is.null(bounds)) bounds <- list(
      alpha_1_raw      = c( qlogis(.02),      qlogis(.98) ),
      alpha_2_raw      = c( qlogis(.02),      qlogis(.98)),
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
      opts = list(algorithm="NLOPT_LN_COBYLA", maxeval=500, xtol_rel=1e-8)
    )
  }
  starts <- replicate(K, make_start(eta0, jitter=0.8), simplify = FALSE)
  fits <- lapply(1:K, function(i){print(i); fit_from_start(starts[[i]])})
  tol <- 1e-6
  feasible <- vapply(fits, function(fr) {
    gi <- try(eval_g_ineq(fr$solution), silent=TRUE)
    is.numeric(gi) && all(gi <= tol)
  }, logical(1))
  objvals <- vapply(fits, function(fr) fr$objective, numeric(1))
  pick <- if (any(feasible)) which.min(replace(objvals, !feasible, Inf)) else which.min(objvals)
  best_fit <- fits[[pick]]
  eta_hat = best_fit$solution
  return(eta_hat)
}
ll_total = function(eta_num){-calc_log_likelihood(eta_num, 'calc')}
ll_vec = function(eta_num){calc_log_likelihood(eta_num, 'diagnostic')$output_df[!is.na(ll)]$ll}

## run from ten different starting points
eta_hat = suppressWarnings(k_initial_runs(10))
n_obs  <- length(ll_vec(eta_hat))

### START POLISHING 
ll_over_n <- function(z) calc_log_likelihood(z, 'calc') / n_obs
penalized_ll_over_n <- function(z, lambda = 10) {
  over <- pmax(z - upper, 0); under <- pmin(z - lower, 0)
  ll_over_n(z) + lambda * sum(over^2 + under^2)
} ## Helper: soft box penalty for Nelder-Mead


v0 <- ll_over_n(eta_hat)
eta_star <- eta_hat
best_val <- v0
moved <- FALSE

## 1) L-BFGS-B letting optim finite-diff the gradient
opt1 <- optim(par = eta_star, fn = ll_over_n, gr = NULL, method = "L-BFGS-B",
              lower = lower, upper = upper,
              control = list(pgtol = 1e-6, maxit = 2000))

cat(sprintf("[L-BFGS-B fd] conv=%d; f=%.6f; iters=%d; msg=%s\n",
            opt1$convergence, opt1$value, opt1$counts[['function']], opt1$message %||% ""))
if (is.finite(opt1$value) && opt1$value < best_val - 1e-10) {
  eta_star <- opt1$par; best_val <- opt1$value; moved <- TRUE
}

## 2) If no meaningful improvement, try Nelder–Mead on penalized objective
if (!moved) {
  opt2 <- optim(par = eta_star, fn = penalized_ll_over_n, method = "Nelder-Mead",
                control = list(reltol = 1e-8, maxit = 5000))
  cat(sprintf("[Nelder-Mead] conv=%d; f_pen=%.6f; iters=%d; msg=%s\n",
              opt2$convergence, opt2$value, opt2$counts[['function']], opt2$message %||% ""))
  # project back into box just in case:
  cand <- pmin(pmax(opt2$par, lower), upper)
  v2 <- ll_over_n(cand)
  if (is.finite(v2) && v2 < best_val - 1e-10) {
    eta_star <- cand; best_val <- v2; moved <- TRUE
  }
}

## 3) Last resort: unconstrained reparam with BFGS
if (!moved) {
  # affine map u -> z in (lower, upper): z = lower + (upper-lower) * logistic(u)
  L <- lower; U <- upper; W <- (U - L)
  to_z   <- function(u) L + W * (1 / (1 + exp(-u)))
  from_z <- function(z) {
    y <- pmax(pmin((z - L) / W, 1 - 1e-12), 1e-12)
    log(y) - log1p(-y)
  }
  v_u <- function(u) ll_over_n(to_z(u))
  u0  <- from_z(eta_star)
  
  opt3 <- optim(par = u0, fn = v_u, method = "BFGS",
                control = list(reltol = 1e-8, maxit = 5000))
  cat(sprintf("[BFGS unconstrained] conv=%d; f=%.6f; iters=%d; msg=%s\n",
              opt3$convergence, opt3$value, opt3$counts[['function']], opt3$message %||% ""))
  cand <- to_z(opt3$par)
  v3 <- ll_over_n(cand)
  if (is.finite(v3) && v3 < best_val - 1e-10) {
    eta_star <- cand; best_val <- v3
  }
}

g_star <- tryCatch(numDeriv::grad(ll_over_n, eta_star, method="Richardson", method.args=list(eps=1e-5)), error=function(e) rep(NA_real_, length(eta_star)))
g_star_n <- if (all(is.finite(g_star))) sqrt(sum(g_star^2)) else NA_real_
cat(sprintf("Final: f=%.6f, ||grad||=%s\n", best_val, ifelse(is.na(g_star_n), "NA", format(g_star_n, digits=6))))



## --- Compute Standard Errors  ----------------------------
theta_from_eta = function(eta_num){as.numeric(calc_log_likelihood(eta_num, 'diagnostic')$output_params)}
invert <- function(M) { co <- try(solve(M), silent = TRUE);if (inherits(co, "try-error")) MASS::ginv(M) else co}

# mean-NLL gradient norm (are we at a stationary point?)
g  <- numDeriv::grad(ll_over_n, eta_star, method = "Richardson",
                     method.args = list(eps = 1e-6))
cat("||grad_mean|| =", sqrt(sum(g^2)), "\n")

# OPG information and its condition number
J    <- numDeriv::jacobian(ll_vec, eta_star, method = "Richardson",
                           method.args = list(eps = 1e-6))
n    <- nrow(J)
Iopg <- crossprod(J) / n
Iopg <- 0.5 * (Iopg + t(Iopg))   # symmetrize

ev   <- eigen(Iopg, symmetric = TRUE, only.values = TRUE)$values
print(ev)
cond <- max(ev) / min(ev)
cat("I_opg eigmin =", min(ev), " eigmax =", max(ev), "  cond =", cond, "\n")



# --- OPG "classical" + clustered, no Hessian ---
n_obs  <- length(ll_vec(eta_star))
J      <- numDeriv::jacobian(ll_vec, eta_star, method="Richardson",
                             method.args=list(eps=1e-6))     # n x k scores

I_opg  <- crossprod(J) / n_obs                               # per-obs info
I_opg  <- 0.5*(I_opg + t(I_opg))                             # symmetrize
V_eta_CR <- MASS::ginv(I_opg) / n_obs                        # "classical"

# Cluster sandwich using same scaling
cluster_id <- calc_log_likelihood(eta_star,'diagnostic')$output_df[!is.na(ll)]$ibes_ticker
k <- ncol(J); S <- matrix(0,k,k)
for (g in unique(cluster_id)) {
  s_g <- colSums(J[cluster_id==g,,drop=FALSE]) / n_obs
  S   <- S + tcrossprod(s_g)
}
V_eta_SW <- MASS::ginv(I_opg) %*% S %*% MASS::ginv(I_opg)

# Delta to Theta
G <- numDeriv::jacobian(theta_from_eta, eta_star)
V_theta_CR <- G %*% V_eta_CR %*% t(G)
V_theta_SW <- G %*% V_eta_SW %*% t(G)
se_theta_CR <- sqrt(pmax(diag(V_theta_CR),0))
se_theta_SW <- sqrt(pmax(diag(V_theta_SW),0))

Theta_hat <- calc_log_likelihood(eta_star, 'diagnostic')$output_params
output_dta <- calc_log_likelihood(eta_star, 'diagnostic')$output_df
se_table <- data.frame(
  param        = names(Theta_hat),
  est          = as.numeric(Theta_hat),
  se_classical = as.numeric(se_theta_CR),
  se_clustered = as.numeric(se_theta_SW)
) %>% mutate(across(-param, ~round(.,5)))
print(se_table)





