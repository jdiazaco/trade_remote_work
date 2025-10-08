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

wrds_query= function(query_string){dbGetQuery(wrds, query_string) %>% data.table()}
getSymbols("^VIX", src = "yahoo", from = "2008-01-01", auto.assign = FALSE) -> VIX_xts
# import IBES data --------------------------------------------------------
importing_IBES = F
if (importing_IBES){
  
  # generate exchange rate data 
  getSymbols("DEXUSEU", src="FRED"); 
  euro_usd_dta = as.data.table(DEXUSEU) %>%  rename(date = index, euro_usd = DEXUSEU) %>% 
    complete(date = seq(min(date), max(date), by = "day"))  %>% 
    fill(euro_usd, .direction = "down")
  
  #### FORECAST DATA 
  #https://wrds-www.wharton.upenn.edu/pages/get-data/lseg-ibes/ibes-guidance/detail-history/detail/
  french_ibes = unique(wrds_query("select ticker as ibes_ticker from ibes.id_guidance where countrycd = 'EF' or ex_countrycd = 'EF'")) %>% .[,french_ibes := T]
  IBES_forecast_data =  wrds_query("SELECT ticker AS ibes_ticker,usfirm, measure, curr, units, anndats AS fcst_ann_date, prd_yr, prd_mon, val_1, val_2, 
                                 mean_at_date AS analyst_forecast_mean FROM ibes.det_guidance 
                                 WHERE pdicity = 'ANN' AND val_1 IS NOT NULL AND measure = 'SAL' AND curr IN ('USD', 'EUR')")  %>% 
    merge(french_ibes, all.x = T) %>% .[is.na(french_ibes), french_ibes := F] %>%
    #.[(usfirm == 1 & curr == "USD") | (french_ibes == T & !is.na(curr))] %>% 
    
    ## clean up how the forecast is reported 
    .[,forecast := apply(.SD, 1, mean, na.rm = TRUE), .SDcols = c('val_1','val_2')] %>% 
    .[!is.na(val_2), `:=`(forecast_lb = val_1, forecast_ub = val_2, forecast_range = val_2 - val_1)] %>% 
    .[forecast_range < 0, paste0('forecast', c('', '_lb', '_ub', '_range')):= NA] %>% 
    .[forecast_range == 0 ,  paste0('forecast', c('_lb', '_ub', '_range')):= NA] %>% 
    .[,c('val_1', 'val_2') := NULL]  %>% 
    
    ## remove forecasts that appear after the fact and those that are over a year away 
    .[, end_of_period := ymd(paste(prd_yr, prd_mon, 1, sep = "-")) %m+% months(1) - days(1)] %>% 
    .[fcst_ann_date <= end_of_period & end_of_period - fcst_ann_date < 365] 
  
    ### CALCULATE THE VIX OVER FORECAST TIME HORIZON 
    vix_IBES = IBES_forecast_data[, c('ibes_ticker', 'fcst_ann_date', 'end_of_period')]
    vix_daily =  data.table(date = index(VIX_xts), vix  = as.numeric(Cl(VIX_xts)))
    vix_IBES_mean = vix_daily[vix_IBES, on = .(date >= fcst_ann_date, date <= end_of_period),
                              allow.cartesian = TRUE, nomatch = 0L] %>% 
      rename(fcst_ann_date = date ,end_of_period  = date.1) %>% 
      .[, .(fcst_period_mean_vix = NA_mean(vix)), by = .(ibes_ticker, fcst_ann_date, end_of_period)]
    IBES_forecast_data = merge(IBES_forecast_data,vix_IBES_mean, all.x = T, 
                               by = c('ibes_ticker', 'fcst_ann_date', 'end_of_period')) %>% 
      .[,end_of_period:= NULL] %>% 
  
  ## mark the first / last forecast 
  setorder(ibes_ticker, measure, prd_yr, fcst_ann_date) %>% 
  .[, `:=`(first_forecast = .I == .I[1], last_forecast = .I == .I[.N]), by = c('ibes_ticker', 'measure', 'prd_yr')]
  
  ###### ACTUAL VALUES OF THE FORECASTED VARIABLES 
  #https://wrds-www.wharton.upenn.edu/pages/get-data/lseg-ibes/ibes-academic/detail-history/actuals/  
  IBES_actual = import_file("1) data/11_parameter_calibration/raw/1_IBES_actuals_raw.csv") %>% rename_with(~tolower(.)) %>% 
    .[, pends := as.Date(pends)] %>% .[, `:=`(prd_yr = year(pends), prd_mon = month(pends))]  %>%
    rename(ibes_ticker= ticker, realized_value = value) %>% 
    distinct(., ibes_ticker, prd_yr, prd_mon, measure, .keep_all = T) %>% 
    select('ibes_ticker','pends', 'prd_yr', 'prd_mon', 'measure', 'realized_value') 
  
  IBES_initial_combined = merge(IBES_forecast_data, IBES_actual,  by = c('ibes_ticker', 'prd_yr', 'prd_mon', 'measure')) %>% 
    merge(euro_usd_dta, by.x = 'pends', by.y = 'date', all.x = T) %>% 
    .[, forecast_horizon := as.numeric(pends - fcst_ann_date)] %>% 
    .[, fiscal_year := ifelse(prd_mon >5, prd_yr, prd_yr - 1)] %>%
    .[!is.na(ibes_ticker)] %>% 
    .[, pstart := pends %m-% years(1) + days(1)] %>% 
    select(ibes_ticker, fcst_ann_date,fiscal_year,pstart, pends, measure, curr, units, con_fil(., 'forecast'),
           realized_value, usfirm, french_ibes, euro_usd, fcst_period_mean_vix) %>% 
    
    ## handle weird duplicates due to firms changing their reporting timing 
    merge(distinct(., fiscal_year, ibes_ticker, .keep_all = T) %>% .[, c('fiscal_year', 'ibes_ticker', 'pends')], by = c('fiscal_year', 'ibes_ticker', 'pends'))
 
  ### Deflate and rescale values 
  vars_to_deflate = c("analyst_forecast_mean","forecast", "forecast_lb", "forecast_ub", "forecast_range", "realized_value") 
  IBES_initial_combined = IBES_initial_combined[curr == "EUR", (vars_to_deflate) := lapply(.SD, function(x) x*euro_usd), .SDcols = vars_to_deflate]
  IBES_initial_combined = deflate_values_us(IBES_initial_combined, vars_to_deflate, 'pends') %>% 
    .[, (vars_to_deflate) := lapply(.SD, function(x) x*1e6), .SDcols = vars_to_deflate]  %>% 
    
  ## Generate the forecast errors 
  .[, `:=`(abs_firm_log_forecast_error = abs(asinh(realized_value) - asinh(forecast)),
           abs_analyst_log_forecast_error = abs(asinh(realized_value) - asinh(analyst_forecast_mean)),
           abs_analyst_forecast_error = abs(realized_value - analyst_forecast_mean),
           abs_firm_forecast_error = abs(realized_value - forecast))] %>% 
    
  ### Generate a var for initial analyst expectations 
  setorder(.,ibes_ticker, fiscal_year) %>% 
  .[, log_fy_init_analyst_forecast := asinh(analyst_forecast_mean[1]), by = .(ibes_ticker, fiscal_year)]
  
  ### GENERATE VARIANCE DATA 
  ibes_var_dta= IBES_initial_combined[,log_realized_value := asinh(realized_value)] %>% 
    .[first_forecast == T & !is.na(ibes_ticker)] %>% 
    unbalanced_lag(., 'ibes_ticker', 'fiscal_year', c('log_realized_value', 'abs_firm_log_forecast_error'), c(1:4)) %>%
    .[, abs_firm_log_forecast_error_prior3 := apply(.SD,1, NA_mean), .SDcols = paste0('abs_firm_log_forecast_error_lag',c(1:3))] %>% 
    .[, ibes_5y_log_rev_var := apply(.SD, 1, NA_var), .SDcols = paste0('log_realized_value',c("", paste0("_lag", 1:4)))]  %>% 
    select(ibes_ticker, fiscal_year, con_fil(.,'log_rev_var','prior3'))
  
  IBES_initial_combined = merge(IBES_initial_combined,ibes_var_dta, all.x = T, by = c('fiscal_year', 'ibes_ticker'))
 
  write_parquet(IBES_initial_combined, "1) data/11_parameter_calibration/raw/2_IBES_forecast_plus_actuals_raw.parquet")
}



# Import compustat data --------------------------------------------------------
importing_compustat = F
if (importing_compustat){


##### https://wrds-www.wharton.upenn.edu/pages/get-data/linking-suite-wrds/ibes-crsp-link/
ibes_crsp_link = import_file('1) data/11_parameter_calibration/raw/3_crsp_ibes_crosswalk.csv') %>%
  rename(ibes_ticker = TICKER, permno = PERMNO) %>% remove_if_NA('permno', 'ibes_ticker') %>% select(-SCORE, NCUSIP) %>% 
  .[is.na(edate), edate := as.IDate("9999-12-31")] %>% .[,`:=`(sdate = as.IDate(sdate), edate = as.IDate(edate))] %>% 
  setkey(., permno, sdate, edate)

##### Import compustat x crsp data https://wrds-www.wharton.upenn.edu/users/saved-query/6357767/
compustat_dta =  import_file('1) data/11_parameter_calibration/raw/4_compustat_data_raw.csv') %>% rename(
  compustat_capital = ppent,
  compustat_rev = sale,
  compustat_intangibles_gdwl = gdwl,
  compustat_intangibles_other = intano,
  compustat_total_assets = at,
  compustat_intangible_assets = intan,
  compustat_rnd = xrd,
  compustat_inventories = invt,
  compustat_investment = capx,  
  fiscal_yr_end_month = fyr,
  compustat_emp = emp, 
  compustat_wages = xlr,
  permno = LPERMNO,
  fiscal_year = fyear,
  gvkey = GVKEY) %>% 
  .[,naics := {d <- str_replace_all(as.character(naics), "[^0-9]", ""); d[nchar(d) == 0] <- NA_character_; str_pad(d, 6, pad = "0")}] %>% 
  .[, naics_2d := substr(naics,1,2)] %>% 
  .[, fiscal_yr_enddate := ymd(paste(fiscal_year + ifelse(as.numeric(fiscal_yr_end_month) < 6, 1, 0), fiscal_yr_end_month, 1, sep = "-")) %m+% months(1) - days(1)] %>% 
  .[, fiscal_yr_startdate := fiscal_yr_enddate %m-% years(1) + days(1)] %>% 
  
  ### discard obs with missing or neg emp, rev, or capital
  .[!(is.na(compustat_emp) | compustat_emp <=0) ] %>% 
  .[!(is.na(compustat_rev) | compustat_rev <=0) ] %>% 
  .[!(is.na(compustat_capital) | compustat_capital <0)] %>%
  
  ## select interest vars  
  select(permno,gvkey, cusip, naics, naics_2d, fiscal_year,fiscal_yr_startdate, fiscal_yr_enddate, fiscal_yr_end_month, 
         con_fil(., 'compustat'), ipodate,datadate) %>% 
  distinct(permno, fiscal_year, .keep_all = T) 

  ## deflate nominal values and convert from millions to actual 
  vars_to_deflate =  setdiff(con_fil(compustat_dta, 'compustat'), "compustat_emp")
  compustat_dta = deflate_values_us(compustat_dta, vars_to_deflate, 'fiscal_yr_enddate') %>% 
  .[, (vars_to_deflate) := lapply(.SD, function(x) x*1e6), .SDcols = vars_to_deflate] %>% 
  
  ## generate the rolling variance of log sales 
  .[, log_compustat_rev := asinh(compustat_rev)] %>% 
  unbalanced_lag(., 'permno', 'fiscal_year', 'log_compustat_rev', 1:5) %>% 
  .[, log_compustat_rev_var_prior5 := apply(.SD, 1, var, na.rm = TRUE), .SDcols = paste0('log_compustat_rev_lag', 1:5)] %>% 
  .[, log_compustat_rev_var_prior5_strict := apply(.SD, 1, var), .SDcols = paste0('log_compustat_rev_lag', 1:5)] %>% 
  .[, log_compustat_rev_var_prior3 := apply(.SD, 1, var, na.rm = TRUE), .SDcols = paste0('log_compustat_rev_lag', 1:3)] %>% 
  .[, log_compustat_rev_var_prior_3_strict := apply(.SD, 1, var), .SDcols = paste0('log_compustat_rev_lag', 1:3)] %>% 
  select(-all_of(con_fil(con_fil(., 'log_compustat_rev'),'prior', inc = F))) %>% 
  
  ## generate additional variables for analysis 
    .[, compustat_rnd_to_rev_ratio := compustat_rnd / compustat_rev] %>% 
    .[is.na(compustat_intangibles_other) & !is.na(compustat_intangible_assets) & !is.na( compustat_intangibles_gdwl), compustat_intangibles_other := compustat_intangible_assets - compustat_intangibles_gdwl] %>%
    .[, weird_intangibles := compustat_intangible_assets > compustat_total_assets | compustat_total_assets <=0] %>% 
    .[weird_intangibles== F, `:=`(compustat_intangible_asset_fraction = compustat_intangible_assets/ compustat_total_assets,
                               compustat_other_intangible_asset_fraction = compustat_intangibles_other / compustat_total_assets,
                               compustat_goodwill_asset_fraction = compustat_intangibles_gdwl / compustat_total_assets)] %>% 
    .[, weird_intangibles := NULL] %>% 
    .[, compustat_inventory_to_rev_ratio := compustat_inventories/ compustat_rev] %>% 
    .[,finan_or_util := naics_2d %in% c("52", "22")] #flag finance and utilities for removal 

##### IMPORT THE SEGMENT DATA (https://wrds-www.wharton.upenn.edu/users/saved-query/6357705/)
seg_dta = fread("1) data/11_parameter_calibration/raw/5_compustat_segment_dta.csv") %>% 
  setnames(., tolower(names(.))) %>% .[ stype %in% c('OPSEG', 'GEOSEG')] %>% 
  .[, datadate := as.IDate(datadate)] %>% 
  setkey(., gvkey, datadate)

seg_dta = compustat_dta[!is.na(fiscal_yr_end_month) & !is.na(datadate),.(gvkey, datadate, fiscal_yr_end_month) ] %>%
  setkey(., gvkey, datadate) %>% 
  .[seg_dta, roll = T] %>% 
  .[,`:=`(cal_month = month(datadate), cal_year = year(datadate)) ] %>% 
  .[, fiscal_year := ifelse(cal_month >= fiscal_yr_end_month, cal_year, cal_year + 1)] %>% 
  .[order(gvkey, fiscal_year, datadate, srcdate)]  %>% 
  .[, latest_filing := (srcdate == srcdate[.N] & datadate == datadate[.N]), by = .(gvkey,stype, fiscal_year)] %>% 
  .[latest_filing == T]  %>% .[!is.na(fiscal_year)]

seg_export_dta = seg_dta[stype == 'GEOSEG'] %>%
  .[, .(compustat_dom_rev = NA_sum(sales*(geotp == 2)),
       compustat_export1 = NA_sum(salexg),
       compustat_export2 = NA_sum(sales*(geotp == 3))), by = .(gvkey, datadate, fiscal_year)] %>%
  .[, compustat_export_rev := rowSums(.SD, na.rm = TRUE), .SDcols = paste0('compustat_export', 1:2)] %>% 
  deflate_values_us(., con_fil(.,'rev'), 'datadate') %>% 
  .[, c(paste0('compustat_export', 1:2), 'datadate') := NULL] %>% 
  .[, con_fil(.,'rev') := lapply(.SD, function(x) x*1e6), .SDcols = con_fil(.,'rev')] 

seg_opseg_dta = seg_dta[stype == "OPSEG", .(compustat_num_segments = uniqueN(sid)), by = .(gvkey,fiscal_year)]
compustat_combined = compustat_dta %>% 
  merge(seg_opseg_dta, all.x = T, by = c('gvkey', 'fiscal_year'))  %>% 
  merge(seg_export_dta, all.x = T, by = c('gvkey', 'fiscal_year'))

#### Merge to IBES 
ibes_crsp_link = ibes_crsp_link[compustat_combined, on = .(permno, sdate <= datadate, edate >= datadate),nomatch = 0L] %>% 
  .[, .(permno, fiscal_year, ibes_ticker)]
compustat_combined = merge(compustat_combined,ibes_crsp_link, by = c('permno','fiscal_year'), all.x = T) 

### keep only unique permno x year obs as well as unique gvkey x year observations
compustat_combined =  compustat_combined[, count := .N, by = .(permno, fiscal_year)] %>% .[count ==1] %>% .[,count := NULL] 
compustat_combined =  compustat_combined[, count := .N, by = .(gvkey, fiscal_year)] %>% .[count ==1] %>% .[,count := NULL] 
compustat_combined = compustat_combined[,  count := ifelse(is.na(ibes_ticker),1, .N), by = .(ibes_ticker, fiscal_year)] %>% .[count == 1] %>% .[,count := NULL]

## export  
write_parquet(compustat_combined, '1) data/11_parameter_calibration/raw/6_compustat_data_initial_processed.parquet')
}

# import LSEG MA data -------------------------------------------------------------
importing_MA_data = F
if (importing_MA_data){
#https://wrds-www.wharton.upenn.edu/pages/get-data/lseg/sdc/mergers-acquisitions/mergers-and-acquisitions/?saved_query=6367077
MA_data = wrds_query(
"SELECT DISTINCT ON (master_deal_no) -- one value per deal 
master_cusip as target_cusip, dateann as deal_announce_date, 
dateeff as deal_effective_date, UPPER(TRIM(acusip)) as cusip_acquirer, deal_value, master_deal_no as deal_num 
FROM tr_sdc_ma.wrds_ma_details 
WHERE dateann BETWEEN '2000-01-01'::date AND '2025-09-12'::date 
AND  status = 'Completed' 
AND (pctown IS NULL OR pctown > 50) -- they need to end up as majority owner 
AND (form IS NULL OR form in ('Merger', 'Acquisition', 'Acq. Maj. Int.')) 
ORDER BY master_deal_no, dateann DESC -- ensures the latest data only 
") %>% .[is.na(deal_effective_date), deal_effective_date := deal_announce_date] %>% 
  deflate_values_us(., 'deal_value', 'deal_effective_date') %>% 
  .[,deal_value := deal_value*1e6] %>% 
  .[, deal_effective_date := as.IDate(deal_effective_date)] %>% .[,deal_date := deal_effective_date]

MA_data = import_file('1) data/11_parameter_calibration/raw/3_crsp_ibes_crosswalk.csv') %>%
  rename_with(~tolower(.)) %>%
  remove_if_NA('permno') %>% 
  .[,cusip_acquirer := substr(ncusip, 1,6)] %>% .[,c('permno', 'cusip_acquirer', 'sdate', 'edate')] %>% 
  .[MA_data, on = .(cusip_acquirer, sdate <= deal_effective_date, edate >= deal_effective_date),
            allow.cartesian = T, nomatch = 0L] %>%
  .[order(deal_num, sdate, edate)] %>%
  .[, .SD[.N], by = deal_num] %>% 
  .[, c('permno', 'deal_date', 'deal_value')]

MA_data = import_file('1) data/11_parameter_calibration/raw/5_compustat_data_initial_processed.parquet') %>% 
  .[, c('permno','gvkey', 'fiscal_year', 'fiscal_yr_startdate', 'fiscal_yr_enddate')] %>% 
  .[!is.na(permno)] %>% 
  .[MA_data, on = .(permno, fiscal_yr_startdate <= deal_date, fiscal_yr_enddate >= deal_date),
    nomatch = 0L] %>% 
  .[!is.na(gvkey), .(fy_MA_deal_value = NA_sum(deal_value), fy_MA_deals = .N), by = .(fiscal_year, gvkey)]

write_parquet(MA_data, '1) data/11_parameter_calibration/raw/7_LSEG_merger_dta.parquet')
}

# combine and clean financial data -----------------------------------------------------------------
combining_financials = F
if (combining_financials){
IBES = import_file("1) data/11_parameter_calibration/raw/2_IBES_forecast_plus_actuals_raw.parquet")
compustat = import_file('1) data/11_parameter_calibration/raw/6_compustat_data_initial_processed.parquet')
ma_dta = import_file('1) data/11_parameter_calibration/raw/7_LSEG_merger_dta.parquet')

### MERGE AND CLEAN 
  financial_dta = merge(compustat,IBES, all = T,  by = c('ibes_ticker', 'fiscal_year')) %>% 
    .[is.na(fiscal_yr_enddate),  fiscal_yr_enddate := pends] %>% 
    .[is.na(fiscal_yr_startdate), fiscal_yr_startdate := pstart] %>% 
    .[, c('pstart', 'pends') := NULL ] %>% 
    .[finan_or_util == F | is.na(finan_or_util)] %>% 
    .[,first_or_no_forecast := is.na(first_forecast) | first_forecast == T] %>% 
    
    merge(ma_dta, all.x = T, by = c('fiscal_year', 'gvkey')) %>% 
    .[is.na(fy_MA_deals), fy_MA_deals := 0] %>% 
    .[fy_MA_deals ==0, fy_MA_deal_value := 0] %>% 
    .[,log_fy_MA_deal_value := asinh(fy_MA_deal_value)] 
  
    ### add Vix for the fiscal year 
    vix_financial = distinct(financial_dta, fiscal_yr_startdate, fiscal_yr_enddate)
    vix_daily =  data.table(date = index(VIX_xts), vix  = as.numeric(Cl(VIX_xts)))
    vix_financial_mean = vix_daily[vix_financial, on = .(date >= fiscal_yr_startdate, date <= fiscal_yr_enddate),
                            allow.cartesian = TRUE, nomatch = 0L] %>% 
    rename(fiscal_yr_startdate = date , fiscal_yr_enddate  = date.1) %>% 
    .[, .(fiscal_yr_mean_vix = NA_mean(vix)), by = .( fiscal_yr_startdate, fiscal_yr_enddate)]
    financial_dta = merge(financial_dta,vix_financial_mean, all.x = T, by = c('fiscal_yr_startdate', 'fiscal_yr_enddate')) 
  
### # IBES birth (and industry group) match + age vars 
  ibes_birth_ages = wrds_query("SELECT item6003 AS firm_name, item6038 AS ibes_ticker, item6008 AS isin, item6011 AS industry_group, 
                            item18272 AS birth_date, item18273 AS incorporation_date FROM trws.wrds_ws_company WHERE item6038 is not NULL") %>% 
    distinct(ibes_ticker, .keep_all = T) %>% remove_if_NA('ibes_ticker')
  
  financial_dta = merge(financial_dta, ibes_birth_ages, all.x = T, by = 'ibes_ticker') %>% 
    .[,birth_year := year(case_when(!is.na(birth_date) ~ birth_date,
                                    !is.na(incorporation_date) ~ incorporation_date,
                                    !is.na(ipodate) ~ ipodate))] %>% 
    .[fiscal_year >= birth_year,age := 1+ fiscal_year - birth_year]  %>% 
    .[, log_age := log(age)]
    
### ADD THE RCID DATA 
  rcid_matching =  wrds_query("SELECT rcid,child_rcid, ultimate_parent_rcid, year_founded, isin, cusip,
                              hq_country as revel_country, year_founded as revel_birth_year 
                              FROM revelio.company_mapping WHERE isin is not NULL or cusip is not NULL")
  
  financial_dta = financial_dta %>% merge(rcid_matching[!is.na(cusip),c('cusip', 'rcid')] %>% distinct(cusip, .keep_all = T), by = 'cusip', all.x = T) %>% 
    merge(rcid_matching[!is.na(isin),c('rcid', 'isin')] %>% distinct(isin, .keep_all = T) %>% rename(rcid_isin = rcid), by= 'isin', all.x = T) %>% 
    .[is.na(rcid), rcid := rcid_isin] %>% .[,rcid_isin := NULL] 
  
  rcid_flags = financial_dta[first_or_no_forecast == T & !is.na(rcid)] %>% 
    .[, count := .N, by = .(rcid, fiscal_year)] %>%  .[count >1] %>% distinct(rcid, fiscal_year) %>% .[,rcid_flag := T] 
  
  financial_dta = merge(financial_dta, rcid_flags, all.x = T, by = c('rcid', 'fiscal_year')) %>% .[rcid_flag == T, rcid := NA] %>% 
    merge(rcid_matching[!is.na(rcid),c('rcid','child_rcid', 'ultimate_parent_rcid', 'revel_country', 'revel_birth_year')], all.x = T, by = 'rcid') %>% 
    .[,is_ultimate_parent := rcid == ultimate_parent_rcid & !is.na(rcid)] %>% 
    .[fiscal_year >= revel_birth_year,revel_age := 1+ fiscal_year - as.numeric(revel_birth_year)] %>% 
    .[,`:=`(log_revel_age = log(revel_age), revel_age_sq = revel_age^2)] 
    
### ADD DETRENDED VARIANCE DATA  
  compustat_var_dta= financial_dta[,log_compustat_rev := asinh(compustat_rev)] %>% 
    .[first_or_no_forecast == T & !is.na(gvkey) & !is.na(compustat_rev)] %>% 
    .[,obs_count := .N, by = gvkey] %>% 
    .[obs_count > 5, compustat_trend_resid := sub_regression(log_compustat_rev, log_revel_age, residuals = T), by = gvkey] %>% 
    .[, abs_compustat_trend_resid := abs(compustat_trend_resid)] %>% 
    unbalanced_lag(., 'gvkey', 'fiscal_year', 'compustat_trend_resid', 1:3) %>% 
    .[, ("compustat_trend_resid_var_prior3") := apply(.SD, 1,NA_var), .SDcols = paste0("compustat_trend_resid", "_lag",1:3)] %>% 
    .[, ("mean_abs_compustat_trend_resid_prior3") := apply(.SD, 1,function(x) NA_mean(abs(x))), .SDcols = paste0("compustat_trend_resid", "_lag",1:3)] %>% 
    .[,c('gvkey','fiscal_year','compustat_trend_resid','abs_compustat_trend_resid', 
         "compustat_trend_resid_var_prior3" ,"mean_abs_compustat_trend_resid_prior3")]
    
  ibes_var_dta= financial_dta[,log_realized_value := asinh(realized_value)] %>% 
    .[first_or_no_forecast == T & !is.na(ibes_ticker) & !is.na(realized_value)] %>% 
    .[,obs_count := .N, by = ibes_ticker] %>% 
    .[obs_count > 5, ibes_trend_resid := sub_regression(log_realized_value, log_revel_age, residuals = T), by = ibes_ticker] %>% 
    .[, abs_ibes_trend_resid := abs(ibes_trend_resid)] %>% 
    unbalanced_lag(., 'ibes_ticker', 'fiscal_year', 'ibes_trend_resid', 1:3) %>% 
    .[, ("ibes_trend_resid_var_prior3") := apply(.SD, 1,NA_var), .SDcols = paste0("ibes_trend_resid", "_lag",1:3)] %>% 
    .[, ("mean_abs_ibes_trend_resid_prior3") := apply(.SD, 1,function(x) NA_mean(abs(x))), .SDcols = paste0("ibes_trend_resid", "_lag",1:3)] %>% 
    .[,c('ibes_ticker','fiscal_year','ibes_trend_resid','abs_ibes_trend_resid',
         "ibes_trend_resid_var_prior3" ,"mean_abs_ibes_trend_resid_prior3")]
  
  financial_dta= financial_dta %>% 
    merge(ibes_var_dta, all.x = T, by = c( 'ibes_ticker', 'fiscal_year')) %>%
    merge(compustat_var_dta, all.x = T, by = c('gvkey', 'fiscal_year'))
  
write_parquet(financial_dta, "1) data/11_parameter_calibration/clean/1_cleaned_financial_dta.parquet")
}  


# import role data of parent company --------------------------------------
importing_parent_role = T
if(importing_parent_role){
  
  financial_dta = import_file("1) data/11_parameter_calibration/clean/1_cleaned_financial_dta.parquet")
  rcid_years = financial_dta[is_ultimate_parent == T] %>% 
    distinct(rcid, fiscal_year, .keep_all = T) %>% 
    select(rcid, fiscal_year ,fiscal_yr_startdate, fiscal_yr_enddate) %>% .[,og := 0] %>% 
    bind_rows(mutate(., fiscal_year := fiscal_year - 1, across(con_fil(.,'yr'), ~. %m-% years(1)), og = 1)) %>% 
    setorder(fiscal_year, og) %>%  .[, .SD[1], by = .(fiscal_year,rcid)] %>% .[,og := NULL] %>% 
    na.omit()
  
  rcid_list = sample(unique(rcid_years$rcid))
  groups = split(rcid_list, cut(seq_along(rcid_list), breaks = 1000, labels = FALSE))
  int_vars = c('total','data', 'rnd', 'stem')
  out_var_string = paste0('sum(comp*',int_vars,') as comp_',int_vars ) %>% c(., gsub('comp', 'emp', .)) %>% paste(., collapse  = ', ')
  role_dict= import_file('1) data/7_revelio_data/b_processed_data/linkedin/revelio_role_dict.csv')
  payload2 = toJSON(role_dict %>% select('role_k1500', int_vars), dataframe = "rows", auto_unbox = TRUE, na = "null")
  

  ## setup cluster and run in parallel
  plan(multisession, workers = 6)
  future_lapply(1:length(groups), function(i) {
    
    output_file = paste0('1) data/11_parameter_calibration/raw/parent_temp/parent_role_',i,'.csv')
    if (!file.exists(output_file)){
      wrds <- dbConnect(RPostgres::Postgres(), host="wrds-pgdata.wharton.upenn.edu", port=9737,
                        dbname="wrds", user="am0195", password="5KgTCrZ3Vu79.yD", sslmode="require")
      
      # ensure cleanup even on crash or interruption
      on.exit({try(dbDisconnect(wrds), silent = TRUE)}, add = TRUE)
      
      payload1 = toJSON(rcid_years[rcid %in% groups[[i]]], dataframe = "rows", auto_unbox = TRUE, na = "null")
      sql = paste0("
            WITH ext AS (SELECT *  FROM jsonb_to_recordset($1::jsonb) AS
                         x(fiscal_year bigint, rcid bigint, fiscal_yr_startdate date, fiscal_yr_enddate date)),
                         
            role_dict AS (SELECT *  FROM jsonb_to_recordset($2::jsonb) AS x(role_k1500 text,",paste(int_vars, "int", collapse = ", ")," )),
            target_rcids AS( 
                         select e.fiscal_year, cm.rcid as sub_rcid, e.rcid, e.fiscal_yr_startdate, e.fiscal_yr_enddate
                         FROM ext e 
                         JOIN revelio.company_mapping cm on cm.ultimate_parent_rcid = e.rcid
                         ),
                         
            sample AS(
                SELECT t.fiscal_year, t.rcid,",paste0('r.', int_vars, collapse = ", "),",
                ip.total_compensation*ip.weight AS comp, ip.weight as emp, iu.prestige, 
                iu.highest_degree IN ('Master', 'Bachelor', 'MBA', 'Doctor') AS college,
                iu.highest_degree IS NOT NULL AS college_total,
                ROW_NUMBER() OVER (PARTITION BY ip.user_id, t.rcid, t.fiscal_year ORDER BY ip.startdate DESC NULLS LAST) AS rn
                FROM target_rcids t
                JOIN revelio.individual_positions ip ON ip.rcid = t.sub_rcid 
                                                    AND ip.startdate <= t.fiscal_yr_enddate 
                                                    AND (ip.enddate IS NULL OR ip.enddate > t.fiscal_yr_startdate)
                LEFT JOIN revelio.individual_user iu ON iu.user_id = ip.user_id
                LEFT JOIN role_dict r ON ip.role_k1500 = r.role_k1500
                WHERE ip.total_compensation IS NOT NULL AND ip.weight IS NOT NULL
                ORDER BY ip.user_id, t.rcid, t.fiscal_year,ip.startdate DESC NULLS LAST)  
            
            select rcid, fiscal_year, ",
                   out_var_string,",
              AVG(prestige) as avg_prestige,
              SUM(emp*CASE WHEN college THEN 1 ELSE 0 END) / NULLIF(SUM(emp*CASE WHEN college_total THEN 1 ELSE 0 END), 0) AS share_empl_college
              from sample
              WHERE rn = 1
              GROUP BY rcid, fiscal_year")
      tictoc::tic()
      temp = dbGetQuery(wrds, sql, params = list(payload1, payload2))
      tictoc::toc()
      dbDisconnect(wrds)
      fwrite(temp, output_file)
    }
    return('')
  }) 

  
  file_list = list.files(path = '1) data/11_parameter_calibration/raw/parent_temp', full.names = T)
  parent_role_dta = rbindlist(lapply(file_list,import_file), use.names = T, fill = T) %>% 
    mutate(across(gpaste('parent_comp_', c('data', 'stem', 'rnd')), ~./parent_comp_total, .names = "share_{col}")) %>% 
    .[, parent_use_data := parent_comp_data > 0 ]

  ### add data on the extensive margin of first data hiring events 
  add_extensive_margin = function(dta, stub){
  comp = paste0('parent_comp_',stub); 
  lag_comp = paste0(comp, '_lag1');
  cohort = paste0('extensive_hire_cohort_', stub) 
  ever_use = paste0('ever_use_', stub)
  event_study_yr_5y_trim = paste0('event_study_yr_5y_trim_',stub)
  event_study_yr_3y_trim = paste0('event_study_yr_3y_trim_',stub)
  in_event_study = paste0('in_event_study_', stub)
  dta = setorder(dta,rcid,fiscal_year) %>% 
    .[, (ever_use) := NA_max(get(comp)) >0 ] %>% 
    .[, event := get(lag_comp) == 0 & get(comp) >0] %>% 
    .[, (cohort) := NA_min(fiscal_year[event]), by = rcid] %>% 
    .[!is.na(get(cohort)), event_revert := get(comp) == 0 & fiscal_year > get(cohort)] %>% 
    .[, event_revert_yr := NA_min(fiscal_year[event_revert]), by = rcid] %>% 
    mutate(across(con_fil(.,'cohort'), ~replace_na(as.numeric(.),Inf))) %>% 
    .[, event_study_yr := fiscal_year - get(cohort)] %>% 
    .[fiscal_year >= event_revert_yr,  event_study_yr := NA] %>% 
    .[, paste0('event_study_yr_',stub) := event_study_yr] %>%

    
    ## TRIM 
    .[, (event_study_yr_5y_trim) := fifelse(event_study_yr > 5,  5, fifelse(event_study_yr < -6, -6, event_study_yr))] %>% 
    .[, (event_study_yr_3y_trim) := fifelse(event_study_yr > 2,  2, fifelse(event_study_yr < -4, -4, event_study_yr))] %>% 
    .[, (in_event_study) := get(cohort) %in% us_year_range] %>% 
    .[, c('event_study_yr', 'event') := NULL]
  }
  parent_role_dta = add_extensive_margin(parent_role_dta, 'data') 
  
  
 write_parquet(parent_role_dta,'1) data/11_parameter_calibration/raw/8_parent_firm_role_dta.parquet') 
}

# Generate trends in employment over time  --------------------------------
generating_trends = F
if(generating_trends){
financial_dta = import_file("1) data/11_parameter_calibration/clean/1_cleaned_financial_dta.parquet")
### Generate Inputs 
rcid_list = setdiff(unique(financial_dta[is_ultimate_parent == T & usfirm == 1][['rcid']]), NA)
in_clause = paste(sprintf("'%s'", gsub("'", "''", rcid_list)), collapse = ",")
data_roles = import_file('1) data/7_revelio_data/b_processed_data/linkedin/revelio_role_dict.csv')  %>%.[data == T]%>% pull(role_k1500)
data_roles =  paste(sprintf("'%s'", gsub("'", "''", data_roles)), collapse = ",")
data_role_spending_over_time = wrds_query(paste0(
  "WITH target_rcids AS (
      SELECT DISTINCT cm.rcid, cm.ultimate_parent_rcid
      FROM revelio.company_mapping cm
      WHERE cm.ultimate_parent_rcid IN (",in_clause , ")),
   data_roles AS (
      SELECT t.ultimate_parent_rcid  AS rcid,  ip.role_k1500, ip.startdate, ip.enddate,
      ip.total_compensation*ip.weight AS comp, ip.weight AS count
      FROM target_rcids t
      JOIN revelio.individual_positions ip ON ip.rcid = t.rcid
      WHERE ip.total_compensation IS NOT NULL AND ip.startdate IS NOT NULL 
      AND ip.role_k1500 IN (",data_roles ,")
      ),
   years AS (
      SELECT generate_series(DATE '2008-01-01', DATE '2024-01-01', INTERVAL '1 year') AS y0)
      
   SELECT EXTRACT(YEAR FROM y.y0)::int AS year, p.role_k1500, SUM(count) AS count,
          SUM(p.comp) AS comp
          FROM years y
          JOIN data_roles p
          ON p.startdate < (y.y0 + INTERVAL '1 year')       -- started before year end
          AND (p.enddate  >= y.y0 OR p.enddate IS NULL)      -- ended on/after year start
          GROUP BY 1, 2
          ORDER BY 1, 2;
      ")) %>%  .[,share_comp := comp / NA_sum(comp)]
write_parquet(data_role_spending_over_time, '1) data/11_parameter_calibration/data/clean/3_data_role_spending_over_time.parquet') 
}

# generate combined dataset -------------------------------------------------------------------------
combining_dta = T
if(combining_dta){
parent_role_dta = import_file('1) data/11_parameter_calibration/raw/8_parent_firm_role_dta.parquet') 
financial_dta = import_file("1) data/11_parameter_calibration/clean/1_cleaned_financial_dta.parquet") 

combined_dta = merge(financial_dta, parent_role_dta, all.x = T, by = c('rcid', 'fiscal_year')) %>% 
  ### generate an initial value of revenue 
  setorder(., fiscal_year) %>% 
  .[fiscal_year %in% us_year_range & !is.na(compustat_rev), compustat_rev_init := compustat_rev[1], by= .(gvkey)] 
  
  ## log necessary values 
  vars_to_log = con_fil(con_fil(combined_dta,  'comp', 'forecast', 'realize_value'),'log', 'fraction', 'ratio', 'first', 'last', inc = F)
  combined_dta = mutate(combined_dta, across(vars_to_log, ~asinh(.), .names = 'log_{col}')) %>% 
    
  ### restrict to sample range
  .[(measure == 'SAL' | is.na(measure)) & fiscal_year %in% us_year_range] 
    
  #### drop top and bottom 1 percent outliers in terms of employee count match
   model_dta = combined_dta[first_or_no_forecast == T]
   model = feols(data = model_dta, asinh(empl_total) ~ log_compustat_emp)
   non_dropped_obs = setdiff(1:nrow(model_dta),-1*model$obs_selection$obsRemoved)
   model_dta = model_dta[non_dropped_obs, emp_residual := model$residuals] 
   bottom_top = quantile(model_dta$emp_residual, c(.01, .99), na.rm = T)
   model_dta = model_dta[emp_residual < bottom_top[1] | emp_residual > bottom_top[2], drop_parent := T] %>%
     .[, .(gvkey,ibes_ticker,fiscal_year, drop_parent)]
     
     combined_dta = merge(combined_dta, model_dta, all.x = T) %>% 
     .[, drop_parent := replace_na(drop_parent, F)]
     

   ## trim observations from the bottom and top of the forecast error distribution 
   combined_dta = combined_dta %>% .[ ,`:=`(lfe = abs_firm_log_forecast_error, fe =abs_firm_forecast_error)]
   combined_dta = combined_dta[, log_forecast_out_of_range :=  lfe < quantile(lfe, .01, na.rm= T) | lfe > quantile(lfe, .99, na.rm = T), by = fiscal_year]  
   combined_dta = combined_dta[, forecast_out_of_range :=   fe < quantile(fe, .01, na.rm= T) | fe > quantile(fe, .99, na.rm = T), by = fiscal_year]  %>% 
     .[,c('fe', 'lfe') := NULL]
   
write_parquet(combined_dta, "1) data/11_parameter_calibration/clean/2_cleaned_finance_plus_roles.parquet")
}







