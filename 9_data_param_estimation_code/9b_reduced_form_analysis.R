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




# 1 Generate graph of fit ---------------------------------------------------
library(ggpmisc)
data = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>%
  .[first_or_no_forecast == T]
fit_graph = ggplot(
  data, aes(x = asinh(parent_empl_total), y = log_compustat_emp)) +
  geom_point(alpha = 0.1) + stat_poly_eq(formula = y ~ x, aes(label = paste(..rr.label.., sep = "~~~")),parse = TRUE) +
  theme_minimal() + labs(title = "Revelio x Compustat Employee Match", x = "log revelio employees", y = "log compustat employees")

ggsave(paste0(us_output_path, '1_employee_fit_graph.png'), width = 6.5, height = 4.25)

feols(data = data,  asinh(parent_empl_total) ~ log_compustat_emp | rcid + fiscal_year, cluster = ~ibes_ticker)


#1a Generate Word Cloud of the roles  --------------------------------------------------------------------
making_graph = F
if(making_graph){
data = import_file('1) data/11_parameter_calibration/raw/data_role_spending_over_time.parquet') 
comp_2024 = sum(data[year == 2024][['comp']] )

comp_wordcloud_2008 = ggplot(data[year == 2008], aes(label = role_k1500, size = comp/ comp_2024)) +
  geom_text_wordcloud_area() + scale_size_area(max_size = 25) + theme_minimal() +
  labs(title = paste0("2008: total comp. ", round(sum(data[year == 2008][['comp']])*1e-9), ' billion'))

comp_wordcloud_2024 = ggplot(data[year == 2024], aes(label = role_k1500, size = comp/ comp_2024)) +
  geom_text_wordcloud_area() + scale_size_area(max_size = 25) + theme_minimal() +
  labs(title = paste0("2024: total comp. ", round(sum(data[year == 2024][['comp']])*1e-9), ' billion'))


output = comp_wordcloud_2008 + comp_wordcloud_2024  + plot_annotation(title = 'Data Employment Growth 2008-2024')
ggsave(paste0(us_output_path, '2_data_change_graph.png'),output, width = 7, height = 3.7)
} 

# 2 Balance Tests -------------------------------------------------------------------------
finance_vars = c('parent_comp_total', gpaste('share_parent_comp_', c('data','rnd', 'stem')),
                 'parent_share_empl_college', gpaste('compustat_', c('rev', 'capital', 'inventory_to_rev_ratio')),
                 'revel_age', 'abs_firm_forecast_error', 'pct_abs_firm_forecast_error')

var_names = c('payroll total', gpaste('share payroll ', c( 'data', 'rnd', 'stem')), 'pct college degree', 
              'revenue', 'capital', 'inventory to rev. ratio', 'firm age', 'abs. forecast error', 'pct forecast error')
  
financial_data = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>%
  .[,pct_abs_firm_forecast_error := abs_firm_forecast_error/ realized_value] %>% 
  .[first_or_no_forecast == T & !is.na(naics_2d) & !is.na(parent_comp_data)] %>%
  .[parent_comp_data == 0, `:=`(comp_data_quartile = 0, share_comp_data_quartile = 0)] %>% 
  .[parent_comp_data !=0, `:=`(comp_data_quartile = ntile(parent_comp_data, 4),
                               share_comp_data_quartile = ntile(share_parent_comp_data, 4)), 
                               by = .(naics_2d, fiscal_year)] %>%
  .[, pct_forecast_error_quartile := ntile(abs_firm_log_forecast_error,4), by = .(naics_2d, fiscal_year)] %>% 
  mutate(across(con_fil(finance_vars, 'share', 'age','pct', 'ratio', inc =F), ~.*1e-6))

data_balance_table = reshape_to_summary(financial_data[!is.na(share_comp_data_quartile)],finance_vars,'share_comp_data_quartile')
fe_balance_table = reshape_to_summary(financial_data[!is.na(pct_forecast_error_quartile)],finance_vars,'pct_forecast_error_quartile')

label = '3_data_balance_tests'
format_table(summary_table_input = data_balance_table,
             label = label,
             headers = "&&& \\multicolumn{4}{c}{Data Payroll Share Quartile} \\\\",
             coef_names = var_names, 
             divisions_before = 2,
             custom_rows = list('\\textbf{Linkedin Data}','\\textbf{Balance Sheet Data}', '\\textbf{Guidance Data}'),
             custom_row_placement = c(8, 19,28),
             spacer_size = 1,
             notes = 'Quartiles at the naics sector x year-level',
             note_width = .8,
             rescale_factor =  1,
             output_path = paste0(us_output_path, label, '.tex'),make_tex = F)


label = '4_forecast_error_balance_tests'
format_table(summary_table_input = fe_balance_table,
             label = label,
             headers = make_headers(4, "Percent Forecast Error Quartile"),
             coef_names = var_names, 
             custom_rows = list('\\textbf{Linkedin Data}','\\textbf{Balance Sheet Data}', '\\textbf{Guidance Data}'),
             custom_row_placement = c(8, 19,28),
             spacer_size = 1,
             notes = 'Quartiles at the naics sector x year-level.  Quartile 4 represents the largest difference between the log forecast and the log realized value',
             note_width = .5,
             rescale_factor =  1,
             output_path = paste0(us_output_path, label, '.tex'),make_tex = F)
# 3 event study version of data adoption -----------------------------------------------------------------------
combined_dta = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>% 
  .[!is.na(gvkey) & first_forecast ==T]

base_controls = paste(c("", 'log_parent_comp_total', 'log_forecast_horizon','parent_share_empl_college',
                      'parent_avg_prestige', 'log_revel_age', 'abs_analyst_log_forecast_error'),  collapse = " + ")

additional_controls = c("","log_compustat_rev", 'log_comp_stem + log_comp_non_data_rnd') %>% c(., paste(.,collapse = "+"))
base_command = reg_command('combined_dta', 
                           'abs_firm_log_forecast_error', 'sunab(extensive_hire_cohort_data,event_study_yr_3y_trim_data)', base_controls,
                           fe = "| ibes_ticker + fiscal_year", cluster = 'ibes_ticker') 

base = eval(parse(text= base_command))
iplot(base)


summary(base, agg = "ATT")


# 4 generate graphs for forecasting ability -----------------------------------------------------------------------
# 1) Load + filter
dta <- import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>% 
  .[!is.na(abs_firm_forecast_error) & usfirm == 1 & !is.na(parent_comp_data) & first_forecast == T & !log_forecast_out_of_range == T]

# 2) Size-adjust the absolute log error
model = feols(abs_firm_log_forecast_error ~ poly(log_realized_value, 2), data = dta)
non_dropped_obs = setdiff(1:nrow(dta),-1*model$obs_selection$obsRemoved)
dta[non_dropped_obs, size_resid_abs_firm_log_forecast_error := model$residuals]

# 3) Make "none/low/high" data-spend groups (by industry × year for the nonzero cases)
dta[parent_comp_data == 0, share_comp_data_quartile := 0L]
dta[parent_comp_data != 0, share_comp_data_quartile := ntile(share_parent_comp_data, 2L), by = .(fiscal_year, naics_2d)]
dta[, quartile_lab := factor(share_comp_data_quartile, levels = c(0, 1, 2), labels = c("none", "low", "high"))]

# 4) Aggregate to series for plotting
dt_q <- dta[, .(value =NA_mean(size_resid_abs_firm_log_forecast_error)), by = .(quartile_lab, fiscal_year)]

# 5) Get VIX (annual mean) and map to right axis
VIX_xts <- getSymbols("^VIX", src = "yahoo", from = "2008-01-01", auto.assign = FALSE)
vix_ann <- data.table(date = index(VIX_xts), vix  = as.numeric(Cl(VIX_xts)))[, .(vix = mean(vix, na.rm = TRUE)), by = .(fiscal_year = year(date))]
rng_val <- range(dt_q$value, na.rm = TRUE)
rng_vix <- range(vix_ann$vix, na.rm = TRUE)
a <- diff(rng_val) / diff(rng_vix)          # scale
b <- rng_val[1] - a * rng_vix[1]            # shift

# 6) Plot: quartiles (left axis) + VIX (right axis), with legend entry for VIX
output = ggplot(dt_q, aes(fiscal_year, value, color = quartile_lab)) +
  geom_line() +
  geom_line(data = vix_ann,
            aes(fiscal_year, a * vix + b, linetype = "VIX"),
            linewidth = 0.7, inherit.aes = FALSE) +
  scale_y_continuous(
    name = "Size Adjusted Mean\nLog Forecast Error",
    sec.axis = sec_axis(~ (. - b) / a, name = "VIX (annual mean)")
  ) +
  scale_color_brewer(palette = "Set1", name = "data payroll\nshare") +
  scale_linetype_manual(name = NULL, values = c("VIX" = "dashed")) +
  labs(x = NULL, title = "Impact of Data Spending on Forecast Errors") +
  theme_minimal() +
  theme(legend.position = "bottom")
ggsave(paste0(us_output_path, '3_forecast_error_performance.png'),output, width = 7, height = 5)


# forecast error regression results  --------------------------------------------
forecast_error_spline_wrapper = function(dta,ind_var, running_var, fe){
  bottom_top = quantile(dta[[running_var]], c(.01,.99), na.rm = T)
  dta = dta[get(running_var) > bottom_top[1] & get(running_var) < bottom_top[2]]
  command =  gsub('comp_data',paste0("comp_data*bs(",running_var,", df = 5)"), base_prt_command) %>%
    gsub('combined_dta', 'dta',.)
  if(ind_var == 'pct')command = gsub("log_abs_(firm|analyst)", "abs_\\1_log", command) %>% gsub('forecast_out','log_forecast_out',.)
  
  if (fe == 'firm') command = gsub('\\| industry_group', '| ibes_ticker',command)
  spline_reg = eval(parse(text = command))
  output_graph = spline_analysis(dta, spline_reg, 'log_parent_comp_data', running_var)
}

combined_dta = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>% 
  .[ usfirm == T & first_forecast == T] %>%
  .[, leave_out_industry_mean_forecast_error_pct :=
      ifelse(.N >2, (NA_sum(abs_firm_log_forecast_error) - abs_firm_log_forecast_error)/(.N-1), NA),
    by = .(industry_group, fiscal_year)] %>% 
  .[,industry_error_quantile := factor(ntile(leave_out_industry_mean_forecast_error_pct, 4)), by = fiscal_year]
  
parent_controls <- paste("", "log_parent_comp_total", "parent_share_empl_college", "parent_avg_prestige",
                          "log_forecast_horizon", "log_revel_age", 'log_fy_init_analyst_forecast',
                         'log_abs_analyst_forecast_error', sep = " + ")

additional_controls = c('', 'log_parent_comp_rnd + log_parent_comp_stem')
base_prt_command = reg_command('combined_dta[forecast_out_of_range== F]','log_abs_firm_forecast_error', 'log_parent_comp_data ',parent_controls,
                               fe ="| ibes_ticker + fiscal_year", cluster = 'ibes_ticker')

variations = expand(
  c('pct', 'abs'),c(F,T), "firm",  additional_controls,
  names = c( 'dep_var','remove_analyst', 'fe', 'add_control')) %>%
  .[,command := base_prt_command] %>% 
  .[remove_analyst == T, command := gsub('\\+ log_abs_analyst_forecast_error', '', command)] %>% 
  .[dep_var == 'pct', command := gsub("log_abs_(firm|analyst)", "abs_\\1_log", command) %>% gsub('forecast_out','log_forecast_out',.)] %>%
  .[fe == 'industry', command := gsub('\\| ibes_ticker', '| industry_group',command)] %>% 
  .[add_control != '',command := mapply(function(c,command) gsub('\\|', paste0("+",c, " |"), command), add_control, command)] %>%
  .[!(remove_analyst == T & add_control !='')]
  
model_output = evaluate_variations(variations)$model_output   


### Most pronounced in industry-years that are very uncertain 
industry_forecast_error_result_pct = forecast_error_spline_wrapper(combined_dta,'pct', 'leave_out_industry_mean_forecast_error_pct', 'firm' ) +
  labs(x = 'industry avg. relative forecast error', y= 'coef. value', title = 'Data Spending Impact on Relative Forecast\nErrors x Industry Uncertainty') +
plot_annotation(caption = paste0(
    "The coefficient value represents the partial derivative of log data spend on relative forecast error ",
    "at given points \nin the distribution of the leave-out industry average relative forecast error. ",
    "It is constructed by interacting log data\nspend with a flexible spline of the industry average with 5 degrees of freedom.")) &
  theme(plot.caption = element_text(hjust = 0),plot.caption.position = "plot")


industry_forecast_error_result_abs = forecast_error_spline_wrapper(combined_dta,'abs', 'leave_out_industry_mean_forecast_error_pct', 'firm' ) +
  labs(x = 'industry avg. log forecast error', y= 'coef. value', title = 'Data Spending Impact on Log Absolute Forecast\nErrors x Industry Uncertainty') +
  plot_annotation(caption = paste0(
    "The coefficient value represents the partial derivative of log data spend on Log absolute forecast error ",
    "at given points \nin the distribution of the leave-out industry average relative forecast error. ",
    "It is constructed by interacting log data\nspend with a flexible spline of the industry average with 5 degrees of freedom.")) &
  theme(plot.caption = element_text(hjust = 0),plot.caption.position = "plot")


ggsave(paste0(us_output_path, 'industry_FE_pct_error_spline.png'),
       industry_forecast_error_result_pct, width = 8.39, height = 5.92)
ggsave(paste0(us_output_path, 'industry_FE_abs_error_spline.png'),
       industry_forecast_error_result_abs,width = 8.39, height = 5.92)

quantile(combined_dta$leave_out_industry_mean_forecast_error_pct, c(1:3,3.88)/4, na.rm= T)
feols(data = combined_dta[log_forecast_out_of_range== F], abs_firm_log_forecast_error~
        log_parent_comp_data*industry_error_quantile + log_parent_comp_total+
        parent_share_empl_college + parent_avg_prestige + log_forecast_horizon +
        log_revel_age + abs_analyst_log_forecast_error| ibes_ticker + fiscal_year,cluster = ~ibes_ticker)


#### IMPACT INVENTORY TO REV 
rev_dta = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>%
  .[first_or_no_forecast == T & !is.na(gvkey)] %>% 
  .[,obs_count := .N, by = ibes_ticker] %>% .[,log_revel_age_sq := log_revel_age^2] %>% 
  .[obs_count > 5, detrended_var := abs(
    sub_regression(log_compustat_rev, log_revel_age,log_parent_comp_total ,log_compustat_capital,log_revel_age_sq, residuals = T)), by = gvkey] %>% 
  .[, `:=`(industry_detrended_var = NA_mean(detrended_var),
           industry_rev = NA_mean(compustat_rev),
           count = .N), by = .(naics_2d,fiscal_year)] %>% 
  .[!is.na(detrended_var), industry_detrended_var := industry_detrended_var -detrended_var/count] %>%
  .[!is.na(compustat_rev), industry_rev := industry_rev - compustat_rev / count ] %>% 
  .[, log_industry_rev := log(industry_rev) ]

rev_reg = list()
rev_reg[[1]] = feols(data = rev_dta, log_compustat_rev~log_parent_comp_data +log_parent_comp_total + parent_share_empl_college + 
                       parent_avg_prestige + log_revel_age + log_compustat_capital| gvkey + fiscal_year,cluster = ~gvkey)

rev_reg[[2]] = feols(data = rev_dta, log_compustat_rev~log_parent_comp_data +log_parent_comp_total + parent_share_empl_college + 
                       parent_avg_prestige + log_revel_age + log_compustat_capital +
                       log_parent_comp_stem + log_parent_comp_rnd| gvkey + fiscal_year,cluster = ~gvkey)

rev_reg[[3]] = feols(data = rev_dta, log_compustat_rev~log_parent_comp_data*industry_detrended_var +log_parent_comp_total + parent_share_empl_college + 
                       parent_avg_prestige + log_revel_age + log_compustat_capital +
                       log_parent_comp_stem + log_parent_comp_rnd| gvkey + fiscal_year,cluster = ~gvkey)


#### MAKE REGRESSION OUTPUT TABLES 

coef_names = c('log payroll data', 'log payroll total', 'share empl. college', 'avg. empl. prestige', 
                 'log fcst horizon', 'log age', 'log E[revenue]', 'analyst fcst error',
                 'log payroll rnd', 'log payroll stem',  'analyst fcst error')
label = 'fcst_error_impact'
format_table(model_output,label = label, caption = 'Impact of Data Spending on Next Period Forecast Errors',
             coef_names = coef_names,
             make_tex = F, make_pdf = T,
             headers =make_headers(3, c('Abs Relative Forecast Error', 'Log Abs. Forecast Error')),
             output_path = paste0(us_output_path, label, '.pdf'),
             rescale_factor = 1,
             note_width = .9,
             divisions_before = 4,
             notes = paste0("Robust standard Errors clustered at the firm level. All regressions include firm and year fixed effects. ",
                            'Log absolute forecast error given by the log of the absolute value of the forecast miss.  ',
                            "Relative forecast error given by the difference between the log of the forecast and the log of the",
                            "observed value."))

rev_coef_names = c('log payroll data', 'log payroll total', 'share empl. college', 'avg. empl. prestige', 
                   'log age', 'log capital', 'log payroll stem', 'log payroll rnd', 'abs. industry resid variation',
                   'log payroll data \\nx abs. industry resid variation')

label = 'rev_impact'
format_table(rev_reg,label = label, caption = 'Impact of Data Spending on Log Revenue',
             coef_names = rev_coef_names,
             custom_row_placement = 25, make_tex = F, make_pdf = T,
             note_width = .8,
             notes = paste0("Robust standard Errors clustered at the firm level. All regressions include year firm and year fixed effects.",
                            " Industry residual variation defined as the leave-out average across firms in the naics 2d sector of the residual",
                            "for the current year of the firm specific regression of log revenue on log age, log age squared, log total payroll,",
                            "and log capital."),
             output_path = paste0(us_output_path, label, '.pdf'))

# h -----------------------------------------------------------------------
combined_dta = import_file("1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet") %>% 
  .[!is.na(forecast) & usfirm == 1 & log_forecast_out_of_range == F] %>% 
  setorder(ibes_ticker, fiscal_year, )







