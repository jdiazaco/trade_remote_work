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
eval_g_ineq <- function(x){
  params=  calc_log_likelihood(x, 'diagnostic')[['output_params']]
  g =  params$A_bar_nonneg_qual- params$A_bar_calc
  return(g)  
}

# import and clean data -----------------------------------------------------------------------
creating_dta = T
if (creating_dta){
  min_streak_length = 8
  min_age_for_likelihood = 3
  vars_to_windsorize = c('L_t', 'Ex_t', 'FE_t')
  top_data_cutoff = .9
  
  dta = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>% 
    .[,comp_data_lag1 := NULL] %>% 
    .[first_forecast == T & fiscal_year %in% us_year_range] %>%  
    .[!log_forecast_out_of_range== T, FE_t := (realized_value - forecast)*1e-6] %>%
    unbalanced_lag('ibes_ticker', 'fiscal_year', c('forecast', 'comp_data'), 1) %>% 
    .[, `:=`(lacks_needed_info = is.na(comp_data_lag1) | is.na(forecast_lag1),
             L_t = comp_data*1e-6,
             Ex_t = forecast*1e-6,
             x_t = realized_value*1e-6)] %>% 
    setorder(ibes_ticker, fiscal_year) %>%
    .[,streak_id := cumsum(lacks_needed_info)] %>% 
    .[, .(ibes_ticker, streak_id, lacks_needed_info,fiscal_year,FE_t,x_t, L_t, Ex_t)] %>% 
    .[,streak_length :=.N, by = streak_id] %>%
    .[streak_length >= min_streak_length] %>% 
    .[,  x_bar := NA_median(x_t), by = ibes_ticker] 
    
    lb_ub = quantile(distinct(dta, streak_id, .keep_all = T)$x_bar,c(.01,.99), na.rm = T)
    dta = dta %>% 
    .[x_bar <= lb_ub[2] & x_bar>=lb_ub[1]] %>% 
    .[, idx := .I] %>% 
    .[, `:=`(age = seq_len(.N) - 1L, prev_row_idx = shift(idx, 1)), by = streak_id] %>% 
    .[, likelihood_eligible := age>=min_age_for_likelihood & !is.na(FE_t)] %>% 
    .[, pair_eligible := likelihood_eligible & lag(likelihood_eligible), by = streak_id] %>% 
    .[, (vars_to_windsorize) := lapply(.SD, function(x) winsorize_relative(x, x_bar,.05, .95, 'ratio')), .SDcols = vars_to_windsorize] %>% 
    .[, .(likelihood_eligible,pair_eligible,ibes_ticker, streak_id, fiscal_year,age, idx, prev_row_idx, x_bar,FE_t, L_t, Ex_t)]
 
  write_parquet(dta, "1) data/11_parameter_calibration/clean/4_calibration_input_awk.parquet")
}
dta = import_file("1) data/11_parameter_calibration/clean/4_calibration_input_awk.parquet")


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
pair_el = dta$pair_eligible
x_bar = dta$x_bar
EPS    <- 1e-10
n = nrow(dta)
eta0 = list(alpha_1_raw = qlogis(.5),
            alpha_2_raw = qlogis(.1), 
            Q_raw = softplus_inv(.66),
            sig2_a_raw = softplus_inv(.66),
            theta_raw = qlogis(.45),
            f0_raw = qlogis(.5))
eta_names = names(eta0)
eta = eta0
map_params = function(eta, theta_max = 1-1e-5, alpha_max = 1-1e-4){
  names(eta) = eta_names
  list(
    alpha_1 = alpha_max *logistic(eta[["alpha_1_raw"]]),
    alpha_2 = alpha_max*logistic(eta[["alpha_2_raw"]]),
    Q     = 1e-4 + softplus(eta[["Q_raw"]]),
    sig2_a     = 1e-4 + softplus(eta[["sig2_a_raw"]]),
    theta = theta_max * logistic(eta[["theta_raw"]]),
    f0      = logistic(eta[["f0_raw"]])   
  )
}

bounds <- list(
  alpha_1_raw = c(qlogis(.02), qlogis(.98)),
  alpha_2_raw = c(qlogis(.02), qlogis(.98)),
  Q_raw       = c(softplus_inv(1e-4), softplus_inv(5)),
  sig2_a_raw = c(softplus_inv(1e-4), softplus_inv(5)),
  theta_raw   = c(qlogis(1e-4), qlogis(1-1e-4)),
  f0_raw      = c(qlogis(1e-4), qlogis(1-1e-4))
)
lower <- sapply(bounds, `[`, 1); upper <- sapply(bounds, `[`, 2)
# perform likelihood estimation -------------------------------------------
library('MASS'); library('numDeriv')
calc_log_likelihood = function(eta, mode = c('calc', 'diagnostic')){
  params = map_params(eta)
  for (name in names(params)) assign(name, params[[name]], envir = environment())
  mu_a = 1
  phi_d_tilde =  scale_L^alpha_1*scale_E^alpha_2
  th2 = theta^2
  Sigma_ub <- Q / (1 - th2) #theta is bounded (0,1)

  #Initialize Sigma_tt --> set all firms initial uncertainty to .5*Sigma_ub
  Sigma_t          = numeric(n)
  Sigma_tplus1_t   = numeric(n)
  n_tplus1         = numeric(n)
  
  # initialization at age 0:
  idx                 = rows_by_age[[1]]
  Sigma_t[idx]        = f0 * Sigma_ub
  Sigma_tplus1_t[idx] = Sigma_t[idx]*th2 + Q
  n_tplus1[idx]       = phi_d_tilde * L_t[idx]^alpha_1 * Ex_t[idx]^alpha_2
  Sigma_tplus1_t[idx] = Sigma_tplus1_t[idx] / (1 + Sigma_tplus1_t[idx]*n_tplus1[idx])
  
  ## roll the kalman filter forward 
  for (k in ages[-1]) {
    idx = rows_by_age[[k+1]]
    Sigma_t[idx] = Sigma_tplus1_t[prev_idx[idx]]
    if (k != max(ages)) {
      Sigma_tplus1_t[idx] = Sigma_t[idx]*th2 + Q
      n_tplus1[idx]       = phi_d_tilde * L_t[idx]^alpha_1 * Ex_t[idx]^alpha_2
      Sigma_tplus1_t[idx] = Sigma_tplus1_t[idx] /(1 + Sigma_tplus1_t[idx]*n_tplus1[idx])
    }
  }

  c_t = x_bar[idx_FE] * mu_a * (Sigma_t[idx_FE] + sig2_a)
  std_moment = FE_t[idx_FE]^2 / (2 * c_t^2) - 1
  huber <- function(r, k = 1.5) { pmin(pmax(r, -k), k) }
  sum_out = NA_sum(huber(std_moment)^2)
  
  doing_tuning = T
  if (doing_tuning){
    safe_cor <- function(x, y) {r <- suppressWarnings(cor(x, y, use = "complete.obs")); if (!is.finite(r)) 0 else as.numeric(r)}

    # Standardize using model c_t (as you already do)
    c_idx <- x_bar[idx_FE] * mu_a * (Sigma_t[idx_FE] + sig2_a)
    z     <- FE_t[idx_FE] / (sqrt(2) * c_idx)
    # build adjacent pairs among idx_FE observations
    i_curr2 <- which(pair_el)           # you pre-computed within-streak adjacency
    i_prev2 <- prev_idx[i_curr2]
    mask2   <- idx_FE[i_curr2] & idx_FE[i_prev2]
    
    z_curr <- z[match(i_curr2[mask2], which(idx_FE))]
    z_prev <- z[match(i_prev2[mask2], which(idx_FE))]
    
    rho_z2  <- safe_cor(z_prev^2, z_curr^2)
    lambda_w2 <- 100.0
    pen_whiten <-  lambda_w2 * (rho_z2)^2 * sum(is.finite(z_prev+z_curr))
    sum_out <- sum_out + pen_whiten
  }
  
  if (mode == 'calc'){ return(sum_out)}
  if (mode == 'diagnostic'){
    output = dta[,.(ibes_ticker,x_bar, fiscal_year,age, FE_t,L_t,Ex_t, Sigma_t)] %>% 
      .[idx_FE == T, pct_miss := std_moment]
    params$Sigma_ub = Sigma_ub
    ### HANDLE A_BAR 
    ## A_bar_calculated relies on the formula 
    ## E[x_t] = x_bar*(A_bar -mu_a(Sigma_t + sigma_a^2)) --> we invert and take the average
    ## A_bar_nonneg_qual is the minimum value of A_bar such that A_bar-mu_a(Sigma_ub + sigma_a^2) > 0 (we always have positive quality)
    params$A_bar_calc =  NA_mean(dta$Ex_t[idx_FE] / dta$x_bar[idx_FE] + mu_a*(output$Sigma_t[idx_FE]+sig2_a))
    params$A_bar_nonneg_qual = mu_a*(Sigma_ub+sig2_a)
    return(list(output_df = output, output_params = params))
  }
}
obj <- function(eta) calc_log_likelihood(eta, 'calc')


# run bounded version -----------------------------------------------------
set.seed(43)
run_initial_regs = function(k){
  make_start <- function(eta0, jitter = 0.5){
    x <- as.numeric(eta0)
    names(x) <- names(eta0)
    
    # jitter around eta0 within bounds
    for (nm in names(x)) {
      lo <- bounds[[nm]][1]; hi <- bounds[[nm]][2]; span <- (hi - lo)
      prop_jit <- runif(1, -jitter, jitter)         
      cand <- x[[nm]] + prop_jit * span;  x[[nm]] <- min(hi, max(lo, cand))}
    return(x)
  }
  
  output = rbindlist(lapply(1:k, function(i){
    print(i)
    eta_cand = make_start(eta0)
    output = nloptr::nloptr(
      x0 = eta_cand,
      eval_f = function(x) list(objective = obj(x), gradient = numeric(length(x))),
      eval_g_ineq = eval_g_ineq,
      opts = list(algorithm="NLOPT_LN_COBYLA", maxeval=1000, xtol_rel=1e-8)
    )
    names(eta_cand) = names(eta0)
    params = c(calc_log_likelihood(output$solution, 'diagnostic')$output_params, 
               eta_cand)
    result = as.data.table(params) %>% mutate(across(everything(), ~round(.,4))) %>% 
      .[,reason := output$termination_conditions] %>% 
      .[,val := output$objective] %>% 
      dplyr::select(reason, val, everything())
  })) %>% arrange(val) %>% .[,n_obs := length(L_t[idx_FE])]
  return(output)
}
initial_out = run_initial_regs(20)

eta_star = initial_out[1] %>% dplyr::select(con_fil(., 'raw')) %>% as.numeric()
names(eta_star) = names(eta0)
output = optim(par = eta_star, fn = obj, gr = NULL, method = "L-BFGS-B",
               lower = lower, upper = upper,
               control = list(pgtol = 1e-6, maxit = 1e4))
params = calc_log_likelihood(output$par, 'diagnostic')$output_params


# run unbounded version  --------------------------------------------------



run_initial_regs = function(k){
  make_start <- function(eta0, jitter = 0.5){
    x <- as.numeric(eta0)
    names(x) <- names(eta0)
    
    # jitter around eta0 within bounds
    for (nm in names(x)) {
      lo <- bounds[[nm]][1]; hi <- bounds[[nm]][2]; span <- (hi - lo)
      prop_jit <- runif(1, -jitter, jitter)         
      cand <- x[[nm]] + prop_jit * span;  x[[nm]] <- min(hi, max(lo, cand))}
    return(x)
  }
  output = rbindlist(lapply(1:k, function(i){
    print(i)
    eta_cand = make_start(eta0)
    output = optim(par = eta_cand, fn = obj, gr = NULL, method = "L-BFGS-B",
                   lower = lower, upper = upper,
                   control = list(pgtol = 1e-6, maxit = 2000))
    params = calc_log_likelihood(output$par, 'diagnostic')$output_params
    result = as.data.table(params) %>% mutate(across(everything(), ~round(.,4))) %>% 
      .[,converged := output$convergence == 0] %>% 
      .[,val := output$value] %>% 
      dplyr::select(converged, val, everything())
  })) %>% arrange(val) %>% .[,n_obs := length(L_t[idx_FE])]
  
  return(output)
}

set.seed(43)
initial_out = run_initial_regs(10)
fwrite(initial_out, 'initial_out.csv')





