# Table Cleanup ----------------------------------------------------
reg_command = function(dataset, dep_var,ind_var, controls ="", fe,iv ="", cluster, family = 'feols', weight = '',
                       time_var = ""){
  
  if(iv ==""){
    command = paste0('feols( data = ',dataset, ", ", dep_var, "~", ind_var, controls, fe, ",cluster = ~", cluster, ")")
  }else{
    command = paste0('feols( data = ',dataset, ", ", dep_var, "~1",controls, fe, "|",ind_var, "~", iv,  ",cluster = ~", cluster, ")")
  }
  
  if(family == 'cox'){
    command = paste0("coxph(formula = Surv(", time_var, ",", time_var, "+ 1,", dep_var, ") ~ ", ind_var, controls, '+ strata(',fe ,") ,data = ", dataset,
                     ", cluster = ", cluster, ", control = coxph.control(iter.max = 1000))")
  }else if(family != "feols"){
    command = gsub('feols\\(', 'feglm(', command)
    command =  paste0(substr(command, 1, nchar(command) - 1), ", family = '", family, "')")
  }
  if(weight!='') command = gsub("\\)", paste0(", weights = ~", weight,")"), command)
  return(command)
}
extract_model_vars <- function(cmd_list){
  extract_from_single_command = function(cmd){
   # Turn the string into a call object
  expr <- parse(text = cmd)[[1]]
  args <- as.list(expr)[-1]  # drop the function name
  
  backup = which(sapply(args, function(x) inherits(x, "formula")))[1]
  # 1 Locate the model formula
  if ("formula" %in% names(args)) {
    fml <- args[["formula"]]
  } else if(!is.na(backup)){
    fml = args[[backup]]
  }else{
    fml = args[[2]]
  }
  
  # 2 Extract all vars from that formula
  vars <- all.vars(fml)
  
  # 3 If there's a cluster=… argument, pull its vars too
  if ("cluster" %in% names(args)) {
    cl <- args[["cluster"]]
    if (inherits(cl, "formula")) {
      cl_vars <- all.vars(cl)
    } else {
      # e.g. cluster = firmid  (a bare symbol)
      cl_vars <- as.character(cl)
    }
    vars <- c(vars, cl_vars)
  }
  
  # 4 Inline data‐filter variables
  if ("data" %in% names(args)) {
    d <- args[["data"]]
    if (is.call(d) && identical(d[[1]], as.name("["))) {
      # d is something like `[`(base_data, year <= first_export_year)
      ds_name <- as.character(d[[2]])
      # take all names in that call, drop the dataset name itself
      subset_vars <- setdiff(all.vars(d), ds_name)
      vars <- c(vars, subset_vars)
    }
  }
  
  return(vars)
  }
  return(unique(unlist(lapply(cmd_list, extract_from_single_command))) %>% setdiff(., c('~')))
}
model_to_df = function(model, is_cox){
  if(!is_cox){
    output =  data.frame(regressor = names(model$coefficients)) %>% mutate(
      year =  as.numeric(str_extract(regressor, '\\d{4}')),
      coef = as.numeric(model$coefficients),
      se = as.numeric(summary(model)$se), 
      p_val = round(summary(model)$coeftable %>% as.data.frame() %>% rename_with(~gsub('z','t',.)) %>%  .[, "Pr(>|t|)"],3),
      lb = coef - 1.96*se, ub = coef + 1.96*se)
  }else{
    output = data.frame(regressor = rownames(model$coefficients)) %>% mutate(
      year = as.numeric(str_extract(regressor, '\\d{4}')),
      coef = as.numeric(model$coefficients[,'coef']),
      se = as.numeric(model$coefficients[,'robust se']),
      p_val = round(as.numeric(model$coefficients[,'Pr(>|z|)']),3),
      lb = coef - 1.96*se, ub = coef + 1.96*se)
  }
  return(output)
}
evaluate_variations = function(variations, save_space = T, full_df = T){
  space_saver = function(model, save_space){
    if(save_space){
      model = summary(model)
      objects_to_delete = sapply(model, object.size) %>% as.data.frame() %>% rownames_to_column() %>%
        rename_with(~c('object','size')) %>% filter(size > 10000)  %>% pull(object)
      objects_to_delete = c(objects_to_delete, 'call_env', 'summary_flags')
      for(object in objects_to_delete) model[[object]] = NULL}
    return(model)
  }
  ## setup 
  variations = variations %>% ungroup()
  unique_commmands =  variations %>% distinct(command) %>% mutate(reg_num = 1:nrow(.))
  variations = merge(variations %>% mutate(counter = 1:nrow(.)),unique_commmands) %>% arrange(counter)
  
  ### TRY TO RUN EACH OF THE MODELS 
  int_output = lapply(1:nrow(unique_commmands), function(i){
    command = unique_commmands$command[i]
    model_attempt = tryCatch({eval(parse(text = command))},  error = function(e){e$message})
    
    ### IF THE MODEL FAILED TO RUN 
    if(typeof(model_attempt) == 'character'){
      short_error = case_when(
        grepl('of the formula but', gsub('\\n',' ',model_attempt)) ~ 'missing variable',
        grepl('The dependent variable', model_attempt) & grepl("is a constant", model_attempt) ~ "constant dep. var",
        grepl('are collinear with the fixed|collinear with the\\nfixed effects', model_attempt) ~ "ind vars collinear w/ fe",
        grepl('All observations contain NA values', model_attempt) ~ 'all observations contain NA',
        grepl('No \\(non-missing\\) observations', model_attempt) ~ 'No (non-missing) observations',
        T ~  "")
      failed_output = data.table(counter = i, command = command, reason = model_attempt, short_error = short_error)
      model = list()
    }
    
    ##### IF THE MODEL SUCCESSFULLY RAN 
    if(typeof(model_attempt) != 'character'){
      failed_output = data.table();
      model = space_saver(model_attempt, save_space)  # remove the actual data from the model outputs 
    }
    
    return(list(failed_output = failed_output, model = model))})
  
  ## cleanup results and output 
  failed_output = list(); model_output= list(); variation_output = list()
  for( i in 1:nrow(variations)){
    reg_num = variations$reg_num[i]
    model_output = append(model_output, list(int_output[[reg_num]]$model))
    failed_output = append(failed_output, list(int_output[[reg_num]]$failed_output))
    model_ran = is.null(int_output[[reg_num]]$failed_output)
    
    ## add the data-frame version of results 
    is_cox = grepl('coxph',variations$command[i])
    temp = tryCatch({model_to_df(int_output[[reg_num]]$model, is_cox)}, error = function(e){data.frame()}) %>%
          mutate(counter = i) %>% merge(variations[i,] %>% select(-command))
    variation_output = append(variation_output, list(temp))
  }
  failed_output = rbindlist(failed_output); variation_output = rbindlist(variation_output, fill = T, use.names = T)
  
  # will only actually use the full output if we're doing event studies 
  if(!full_df) variation_output = variations %>% select(-reg_num) 
  
  ## print if any of the regressions failed
  if(nrow(failed_output!=0)){ print(failed_output)}else{print('ran without issues')}
  return(list(variation_output = variation_output, model_output = model_output, failed_output = failed_output ))
}
pause_for_check = function(enforce){
  if (enforce){
  repeat {
  ans <- readline('press any key to acknowledge: ')
  if (nzchar(ans)) return(ans)
  }
  }}

spline_analysis  = function(dta,spline_reg,int_var, running_var){
  
  running_grid = seq(NA_min(dta[[running_var]]), NA_max(dta[[running_var]]), length.out = 150)
  nd = data.table(id = 1:length(running_grid)) %>% .[,con_fil(names(spline_reg$coefficients), int_var, running_var, inc =F ) :=0] %>% 
    .[,(int_var) := 1] %>% .[,(running_var) := running_grid]
  
  # model matrix for the fitted formula (FE are absorbed, so no issues)
  X <- model.matrix(spline_reg, nd)
  
  # pull coefficients & vcov
  b <- coef(spline_reg)
  V <- vcov(spline_reg)
  
  # identify the relevant coefficients
  nm_beta1 = int_var
  escape_rx <- function(x) gsub("([][{}()+*^$.|?\\-])", "\\\\\\1", x)
  int_rx <- escape_rx(int_var)        # e.g. "log_parent_comp_data"
  run_rx <- escape_rx(running_var)    # e.g. "log_compustat_rev"
  # match:  ^<int_var>[:*]bs(<running_var> ... )
  pat <- paste0("^", int_rx, "[:*]bs\\(", run_rx, ".*\\)")
  nm_inter <- grep(pat, names(b), value = TRUE, perl = TRUE)
  
  
  # check
  stopifnot(nm_beta1 %in% names(b), length(nm_inter) >= 2)
  
  # grab the columns in X corresponding to the interaction; with log_data = 1 these equal the spline basis at each rev
  Z <- X[, nm_inter, drop = FALSE]
  
  # marginal effect and delta-method SE:
  # me = beta1 + Z %*% beta_inter
  me <- as.numeric(b[nm_beta1] + Z %*% b[nm_inter])
  
  # Var(me) = Var(beta1) + Z Var(beta_inter) Z' + 2 Z Cov(beta_inter, beta1)
  V11 <- V[nm_beta1, nm_beta1, drop = TRUE]
  V22 <- V[nm_inter, nm_inter, drop = FALSE]
  V12 <- V[nm_inter, nm_beta1, drop = FALSE]   # (k x 1)
  se <- sqrt( V11 + rowSums((Z %*% V22) * Z) + 2 * as.numeric(Z %*% V12) )
  
  df <- data.table(me = me, lo = me - 1.96 * se,hi = me + 1.96 * se) %>% .[,(running_var) := running_grid]
  
  p <- ggplot(df, aes(df[[running_var]], me)) +
    geom_hline(yintercept = 0, linetype = 2) +
    geom_ribbon(aes(ymin = lo, ymax = hi), alpha = 0.20) +
    geom_line() +
    geom_rug(data = dta, aes(x = dta[[running_var]], y = NULL), sides = "b", alpha = 0.25) +
    labs(x = running_var, y = paste0("∂ dep_var / ∂ ", int_var)) + theme_minimal()
  return(p)
}



display_value = function(x, parens = F){
  vapply(x, function(single_x) { 
    if (is.na(single_x)){out = "NA"
    } else if (single_x == 0){ out = '0'
    }else if (abs(single_x) < 1e-3 || abs(single_x) > 1e4) {
      out <-  gsub('e', ' e',gsub('e\\+', 'e', gsub('e\\+0','e', gsub('e-0', 'e-', formatC(single_x, format = "e", digits = 1)))))
    } else if (abs(single_x) < 10) {
      out <- as.character(round(single_x, 3))
    } else if (abs(single_x) < 100) {
      out <- as.character(round(single_x, 2))
    } else if (abs(single_x) < 1000) {
      out <- as.character(round(single_x, 1))
    } else if (abs(single_x) < 10000) {
      out <- as.character(round(single_x, 0))
    } else {
      out <- as.character(single_x)  # fallback
    }
    
    if (parens) out <- paste0("(", out, ")")
    return(out)
  }, character(1))
}
reshape_to_summary = function(base_data, int_vars, key_variable, eliminate_na = F, median = F){
  base_data = base_data[, key_var := get(key_variable)]
  if(eliminate_na) base_data = base_data[!is.na(key_var)]
  
  if (median){
    hi = suppressWarnings(base_data[, c(setNames(lapply(.SD[, ..int_vars], function(x) median(as.double(x), na.rm =T)), int_vars), setNames(lapply(.SD[, ..int_vars], NA_sd), paste0(int_vars, '_sd'))), by = key_var])
  }else{
    hi = suppressWarnings(base_data[, c(setNames(lapply(.SD[, ..int_vars], NA_mean), int_vars), setNames(lapply(.SD[, ..int_vars], NA_sd), paste0(int_vars, '_sd'))), by = key_var])
  }
  hi= hi %>% 
      select(key_var,gpaste(int_vars, c('', "_sd"))) %>%
      pivot_longer(cols = -key_var, names_to = 'var') %>%
      mutate(across(value, ~ifelse(grepl('sd',var), display_value(.,parens = T), display_value(.)))) %>%
      arrange(key_var) %>% 
      pivot_wider(id_cols = c(var), names_from = key_var, values_from = value) %>% 
      mutate(var = ifelse(endsWith(var, 'sd'), '', var))
  return(hi)
}

model_coef = function(model_output, dummy_version = F, cox = F){
  if(!dummy_version){
    if(!cox) return(unique(unlist(lapply(model_output, function(model){names(model$coefficients)}))))
    if(cox)  return(unique(unlist(lapply(model_output, function(model){
      setdiff(rownames(model$coefficients),
              model$coefficients %>% as.data.frame() %>% filter(is.na(coef)) %>% rownames())
    }))))
  }else{
    return(lapply(model_output, function(model) attr(terms(model, stage = 1), 'term.labels')) %>% unlist() %>% unique())
  }
}

comp_coef_names = function(original, new, dummy_version = F){
  if (typeof(original) == 'list'){
    og = model_coef(original, dummy_version)
    if(is.null(og)) og = model_coef(original, T)
    original = og
  }
  # pad if necessary
  max_length <- max(length(original), length(new))
  original <- c(original, rep(NA, max_length - length(original)))
  new <- c(new, rep(NA, max_length - length(new)))
  
  # show the relationship
  View(data.frame(original = original, new = new))
}

make_headers = function(header_span, header_names){
  paste('&',gpaste('\\multicolumn{',header_span,'}{c}{', header_names,'}', collapse_str = '&&'), '\\\\')
}
# format table  -----------------------------------------------------------
format_table = function(model_inputs = NA,summary_table_input = NA,label, coef_names = NA, collapse_coef_names = T,
                        column_names = NA, custom_rows =NA, custom_row_placement = NA,  
                        headers = NA, divisions_before = NA, notes = NA,
                        note_width = .5, output_path = NA, caption = NULL, rescale_factor = NA, spacer_size = NA,
                        coef_order = NA, cox = F,make_pdf= T,make_tex = T, final_commands = NA_character_, file_protect = F,
                        dummy_version = F){
  if(!all_NA(coef_names)) coef_names = gsub('\\\\\\&', 'specialampreplacement',coef_names)
  insert_column_space <- function(input_string,after_column, og_columns) {
    # Count the number of '&' characters
    amp_count <- str_count(input_string, "&")
    
    # If there are at least four '&' characters
    if (amp_count >= after_column) {
      # Locate the positions of all '&' characters
      amp_positions = str_locate_all(input_string, "&")[[1]] 
      
      # Get the position of the fourth '&'
      fourth_amp_position <- amp_positions[after_column, 1]
      
      # Replace the fourth '&' with '& \hspace{5 pt} &'
      modified_string <- str_sub(input_string, 1, fourth_amp_position - 1) %>%
        paste0("& \\hspace{5 pt} &", str_sub(input_string, fourth_amp_position + 1))
      
      return(modified_string)
    }else{
      input_string = input_string %>% gsub( paste0('\\{',og_columns +1, '\\}'),  paste0('\\{',og_columns+2, '\\}'),. ) %>%
        gsub( paste0('\\{',og_columns, '\\}'),  paste0('\\{',og_columns+1, '\\}'),. )
      return(input_string)
    }
  }
  
  ## method for making the base table if we're doing regression analysis
  if (all_NA(summary_table_input)){
    num_columns = length(model_inputs)
    
    ## obtain the coef names 
    {
    if(dummy_version){
     actual = lapply(model_inputs, function(model) attr(terms(model, stage = 1), 'term.labels'))
    }else{
    if(cox){ actual = lapply(model_inputs, function(model){
      setdiff(rownames(model$coefficients),
              model$coefficients %>% as.data.frame() %>% filter(is.na(coef)) %>% rownames())})
    }else{  actual = lapply(model_inputs, function(model){names(model$coefficients)})}}
    actual = actual %>% unlist() %>% unique()
    }

  
    if(!all(is.na(coef_names))){ coef_names = data.frame(actual = actual, in_table = coef_names) 
    }else{ coef_names = data.frame(actual = actual, in_table = actual)}
    unique_table_names = unique(coef_names$in_table)
    output_matrix = matrix(data = '', nrow = 2*ifelse(collapse_coef_names, length(unique_table_names), nrow(coef_names)),
                           ncol = length(model_inputs))
  
    
    for (i in 1:length(model_inputs)){
      if(cox){
        temp_model = model_inputs[[i]]$coefficients %>% as.data.frame() %>% filter(!is.na(coef))
      }else{
        if(dummy_version) temp_model = data.frame(coef_name = actual, coef_display = '1', se_display = "(0)")
        if(!dummy_version){
        temp_model = data.frame(coef = as.numeric(model_inputs[[i]]$coefficients),
                                "se(coef)"= as.numeric(model_inputs[[i]]$se),
                                p = as.numeric(summary(model_inputs[[i]])$coeftable[, "Pr(>|t|)"])) %>%
          rename("se(coef)"= se.coef., 'Pr(>|z|)' = p)
        rownames(temp_model) = names(model_inputs[[i]]$coefficients) 
      temp_model = temp_model %>%
        mutate(across(c(coef, `se(coef)`), ~display_value(.), .names = "{col}_display"),
               coef_display = paste0(coef_display, case_when(`Pr(>|z|)` < .001 ~ "***",`Pr(>|z|)` < .01 ~'**',`Pr(>|z|)` < .05 ~ "*",`Pr(>|z|)` <.1 ~"+",T ~ "")),
               se_display = paste0('(', `se(coef)_display`, ')'))
      temp_model$coef_name = rownames(temp_model)
        }}
      for(j in 1:nrow(temp_model)){
        if(collapse_coef_names){
          temp_row =  coef_names %>% filter(actual ==temp_model$coef_name[j]) %>% pull(in_table) %>% .[1] 
          temp_row  = which(unique_table_names == temp_row)*2
        }else{
          temp_row = which(coef_names$actual == temp_model$coef_name[j])*2
        }
        output_matrix[c(temp_row -1, temp_row),i] = c(temp_model$coef_display[j], temp_model$se_display[j])
      }
    }
    if(collapse_coef_names){ var_names = unique_table_names}else{var_names = coef_names$in_table} 
    var_names = unlist(lapply(var_names, function(x) c(x, "")))
    
    
    
    output_matrix = rbind(c("",gpaste("(", 1:length(model_inputs), ")")),
                          cbind(as.vector(var_names), output_matrix),
                          c("Num. Obs",lapply(model_inputs,function(model){
                            ifelse(cox,as.character(model$n), as.character(model$nobs))}) %>% unlist()))
    
    ## SPLIT APART MULTI-LINE NAMES 
    name_vec = output_matrix[,1]
    for (string_literal in c('\\n', '\\\\n')){
      for (i in (1:length(name_vec))[grepl(string_literal, name_vec)] ){
        split_value = str_split(name_vec[i], string_literal)[[1]]
        name_vec[i] =split_value[1]
        name_vec[i+1] = split_value[2]
      }
    }
    output_matrix[,1] = name_vec
    
    table = capture.output(kable(output_matrix , format = "latex", booktabs = TRUE))
    table[2] =  gsub("\\\\begin\\{tabular\\}\\{l", "", table[2]) %>%
      gsub('l', 'c',.) %>% paste0("\\begin{tabular}{l",.) 
    
    intro_rows = c("", "\\begin{table}[h]",paste0("\\caption{",caption,"}"), "\\begin{center}") 
    end_rows = c(paste0('\\label{',label,'}'), '\\end{center}','\\end{table}')
    table = c(intro_rows, table[-1], end_rows) %>% gsub('toprule', 'hline', .) %>%
      gsub('bottomrule', 'hline',.)
    table =  gsub('\\( ','(', table )
    p_values = paste0("\\multicolumn{", num_columns+1,"}{l}{\\scriptsize{$^{***}p<0.001$; $^{**}p<0.01$; $^{*}p<0.05$; $^{+}p<0.1$}}")
    table = append(table, '\\hline', after =  which(grepl("& \\(1\\) & \\(2\\)", table))[1] )
    table = append(table, '\\hline', after = which(grepl('Num. Obs', table))[1]-1)
    table = append(table, p_values, after = which(grepl('Num. Obs', table))[1]+1)
    
    table = table %>% .[. !="\\addlinespace" ]
  }
  
  ## method for making the base table if we're doing making a summary table 
  if(!all_NA(summary_table_input)){
    output_matrix = summary_table_input
    
    # assign custom coefficient names 
    if(!all_NA(coef_names)){
      name_vec = rep(NA,length(coef_names)*2)
      for(i in 1:length(coef_names)){
        name_inputs = str_split(coef_names[i], '\n')[[1]]
        name_vec[(i-1)*2+1] = name_inputs[1]
        name_vec[(i-1)*2+2] = name_inputs[2]
      }
      name_vec[is.na(name_vec)] = ''
      output_matrix$var = name_vec
    }
    
    num_columns = ncol(output_matrix)- 1 
    table = capture.output(kable(output_matrix , format = "latex", booktabs = TRUE))
    table[4] = gsub('var', "", table[4])
    table[2] =  gsub("\\\\begin\\{tabular\\}\\{l", "", table[2]) %>%
      gsub('l', 'c',.) %>% paste0("\\begin{tabular}{l",.) 
    intro_rows = c("", "\\begin{table}[h]",paste0("\\caption{",caption,"}"), "\\begin{center}") 
    end_rows = c(paste0('\\label{',label,'}'), '\\end{center}','\\end{table}')
    table = c(intro_rows, table[-1], end_rows) %>% gsub('toprule', 'hline', .) %>%
      gsub('bottomrule', 'hline',.) %>% gsub('midrule', 'hline',.)
    table = table %>% .[. !="\\addlinespace" ]
  }
  
  ### fix column names 
  name_line = which(grepl("& \\(1\\) & \\(2\\)", table))[1] 
  if (any(!is.na(column_names))){
    table = table[-name_line]
    num_lines = max(str_count(column_names,"\\n")) +1 
    name_vectors = lapply(1:num_lines, function(i){ rep("", length(column_names))})
    for (i in 1:length(column_names)){
      name = strsplit(column_names[i], '\\n')[[1]]
      for (j in 1:length(name)) name_vectors[[j]][i] = name[j]
    }
    for (i in rev(1:length(name_vectors))){
      table = append(table,paste0(paste(c(" ",name_vectors[[i]]), collapse = "&"),"\\\\"),
                     after = name_line - 1)
    }
  }
  
  ## add generic spacers 
  if (!is.na(spacer_size)){
    if(is.na(name_line)) name_line = 0
    table[(name_line +1):length(table)] =  table[(name_line +1):length(table)] %>%
      gsub(")\\\\", paste0(")\\\\\\\\ [",spacer_size,'em]'),.) %>% gsub('em]\\\\', 'em]',.)
  }
  ### Reorder data rows
  if (!all_NA(coef_order)){
    inner_portion = c()
    for (i in 1:length(coef_order)){
      j = coef_order[i]
      inner_portion = c(inner_portion,(j-1)*2+1, (j-1)*2+2)
    }
    start_of_vars = (which(table == '\\hline')[2] + 1)
    end_of_vars = (which(table == '\\hline')[3] -1)
    table = c(table[1:(start_of_vars - 1)],
              table[start_of_vars:end_of_vars][inner_portion],
              table[(end_of_vars+1): length(table)])
    
  }
  
  ### Add custom rows 
  if (is.na(spacer_size)) table = append(table, "[1em]", after = (which(table == "\\hline")[3]-1))
  if (any(!is.na(custom_rows))){
    if (all_NA(custom_row_placement)){custom_row_placement = rep(NA,length(custom_rows))}
    for (i in 1:length(custom_rows)){
      after_loc = ifelse(is.na(custom_row_placement[i]), (which(table == "\\hline")[3]-1), 
                         custom_row_placement[i])
      table = append(table,paste0(paste(custom_rows[[i]], collapse = " & "),"\\\\"),
                     after = after_loc)
    }
    
  }  
  
  ### Insert Divisions for Ease of Reading 
  if (!all_NA(divisions_before)){
    table[5] = gsub("\\}\\{l",paste0('}{l ', paste(rep('c', length(divisions_before)), collapse = '')), table[5])
    divisions_before = sort(divisions_before)
    for (j in 1:length(divisions_before)){
      division_placement = divisions_before[j] + (j-1)
      for (i in 1:length(table)){
        table[i] = insert_column_space(table[i],division_placement,num_columns + (j-1))
      }
    }
  }
  
  ### Add Headers 
  if (!is.na(headers)){table<- append(table, headers, after = 6)}
  
  ## UPDATE THE NOTES
  if (!all_NA(notes)){
    
    ## if we're doing regression analysis 
    if (all_NA(summary_table_input)){
      notes_index = grep("^{***}p", table, fixed = T)[1]
      note_base = table[notes_index]
      multi_col = regmatches(note_base, regexpr("\\\\multicolumn\\{\\d+\\}\\{l\\}", note_base))
      p_vals =  gsub("\\\\multicolumn\\{\\d+\\}\\{l\\}\\{\\\\scriptsize\\{", "", note_base) %>% 
        gsub("\\}\\}$", "", .)
      note_line = paste0(multi_col,'{\\parbox{',note_width, 
                         '\\linewidth}{\\scriptsize \\vspace{5 pt}', 
                         p_vals, " ", notes, '}}')
      table[notes_index] = note_line}
    
    ## if we're not doing regression analysis 
    if (!all_NA(summary_table_input)){
      pre_notes_index =   grep('end\\{tabular', table)[1] -1
      note_num_columns = num_columns +1; if(!all_NA(divisions_before)) note_num_columns = note_num_columns + length(divisions_before)
      note_line = paste0('\\multicolumn{',note_num_columns,'}{l}{\\parbox{',note_width,
                         '\\linewidth}{\\scriptsize \\vspace{5 pt}',
                         paste(notes, collapse = ""), "}}")
      table = append(table, note_line, after = pre_notes_index)
    }
  }
  
  ### Update so that the table actually shows up where it's supposed to
  table[2] = "\\begin{table}[h]"    
  
  ### remove caption if not using it 
  if (is.null(caption)) table = table[-3]
  if (!is.na(rescale_factor)){
    table = append(table, paste0( "\\begin{adjustbox}{width=",rescale_factor,
                                  "\\textwidth, totalheight=\\textheight-2\\baselineskip,keepaspectratio}"),
                   after = grep("begin{center}", table, fixed = T)[1]) 
    table = append(table, "\\end{adjustbox}", after = grep("end{tabular}", table, fixed = T)[1]) 
  }else{
    table = append(table, "",   after = grep("begin{center}", table, fixed = T)[1]) 
    table = append(table, "", after =  grep("end{tabular}", table, fixed = T)[1])
  }
  
  ## fix the way function interpets special characters 
  table = gsub('textbackslash\\{\\}','', table ) %>% gsub('\\\\\\{', '{',.) %>% gsub('\\\\\\}', '}',.) %>% 
    gsub('specialampreplacement', '\\\\&', .) 
  
  ## add any final command if necessary 
  if (!is.na(final_commands)) eval(parse(text = final_commands))
  
  # output table to file 
  if (!is.na(output_path)){
    hard_write = file_protect & file.exists(output_path) &  grepl('GoogleDrive-amagnuson@g.harvard.edu',getwd())
    if (make_tex){
      if(!hard_write){
        writeLines(table, output_path)
      }else{
        writeLines(table, 'temp.tex');
        drive_update(output_path, 'temp.tex')
        file.remove('temp.tex')
      }
    }
    if (make_pdf){
      latex_preamble <- "\\documentclass[11pt]{article}\\usepackage{adjustbox,amsmath,amsthm,amssymb,enumitem,graphicx,dsfont,mathrsfs,float,caption,multicol,ragged2e,xcolor,changepage,hyperref,printlen,wrapfig,stackengine, fancyhdr,pdflscape,parskip}\\hypersetup{colorlinks=true, linkcolor=blue, filecolor=magenta, urlcolor=blue,}\\usepackage[margin=1in]{geometry}\\usepackage[utf8]{inputenc}\\renewcommand{\\qedsymbol}{\\rule{0.5em}{0.5em}}\\def\\lp{\\left(}\\def\\rp{\\right)}\\DeclareMathOperator*{\\argmin}{arg\\,min}\\DeclareMathOperator*{\\argmax}{arg\\,max}\\def\\code#1{\\texttt{#1}}\\newcommand\\fnote[1]{\\captionsetup{font=small}\\caption*{#1}}\\usepackage[savepos]{zref}\\raggedcolumns\\RaggedRight\\makeatletter \\makeatother\\def\\bfseries{\\fontseries \\bfdefault \\selectfont\\boldmath}\\graphicspath{{./graphics/}}"
      cat(latex_preamble, '\\begin{document}', table, '\\end{document})',file = 'temp.tex')
      tinytex::latexmk("temp.tex")
      pdf_path = gsub('tex', 'pdf', output_path)
      file.remove("temp.tex")
      if(hard_write){
        drive_update(pdf_path, 'temp.pdf')
        file.remove('temp.pdf')
      }else{
      file.rename('temp.pdf', pdf_path)
      }
    }
  }
  
  return(table)
}

# misc --------------------------------------------------------------------
deflate_values_us = function(df, value_vars, date_var, replace = T){
  ### DEFLATOR DATA 
  getSymbols("GDPDEF", src = "FRED")
  deflator_dta = as.data.table(GDPDEF) %>% rename_with(~c('date', 'gdp_def')) %>% 
    complete(date = seq(min(date), max(date), by = "day"))  %>% 
    fill(gdp_def, .direction = "down") %>% as.data.table()
  base_deflator_val = deflator_dta[date == as.Date('2025-01-01')][['gdp_def']]
  deflator_dta[, gdp_def := base_deflator_val/ gdp_def]
  
  if (replace == T){
    df = merge(df, deflator_dta, by.x = date_var, by.y ='date', all.x = T) %>%
      .[, (value_vars) := lapply(.SD, function(x) x*gdp_def), .SDcols = value_vars]
  }else{
    df = merge(df, deflator_dta, by.x = date_var, by.y ='date', all.x = T) %>%
      .[, (paste0(value_vars,'_deflated')) := lapply(.SD, function(x) x*gdp_def), .SDcols = value_vars]  
  }
  df[,gdp_def := NULL]
  return(df)
}

get_largest_polygon = function(geom){ 
  parts = st_cast(geom, 'POLYGON')
  areas = st_area(parts)
  parts[which.max(areas)]}
softplus <- function(x) log1p(exp(-abs(x))) + pmax(x, 0)
logistic <- function(x) 1/(1 + exp(-x))
windsorize = function(dta,int_var, relative_var = NULL, probs){
  if (is.null(relative_var)){
    q = quantile(dta[[int_var]], probs = probs, na.rm = T, type = 7)
    new_var = pmin(pmax(dta[[int_var]], q[1L]), q[2L])
  }else{
    base = dta[[relative_var]]
    scale_factor = dta[[int_var]] / base
    q = quantile(scale_factor, probs = probs, na.rm = T, type = 7)
    scale_factor =  pmin(pmax(scale_factor, q[1L]), q[2L])
    new_var = base*scale_factor
    new_var[base == 0] = NA
  }
  return(new_var)
}
softplus_inv <- function(y) {
  # inverse of log1p(exp(x))
  if (any(y <= 0)) stop("softplus is only defined for y > 0")
  log(exp(y) - 1)
}

gen_dummy_dataset = function(base,subset_vars =NA, discrete_vars = NA, id_vars, rounding_vars = NA, dont_repeat_ids_within =NA){
  
  ## add dummies if necessary
  if (all(is.na(subset_vars))){base$dummy = 1 ; subset_vars = 'dummy'}
  if (all(is.na(dont_repeat_ids_within))){base$dummy_2 =1; dont_repeat_ids_within = 'dummy_2'}
  
  continuous_vars = setdiff(names(base), c(subset_vars, discrete_vars, id_vars, 'dummy', 'dummy_2'))
  print(paste('continuous_vars are: ', paste(continuous_vars, collapse = ", ")))
  subsets = base[,.(count = .N), by = subset_vars] %>% .[count >= 5] %>% .[, `:=`(subset_num = .I,count = NULL)]
  base = merge(base, subsets, by = subset_vars)
  mins = apply(base[,..continuous_vars],2, NA_min); maxes = apply(base[,..continuous_vars],2, NA_max)
  full_discrete = unique(c(subset_vars, discrete_vars, id_vars))
  
  ## generate dummy versions of the id_vars 
  for (id_var in id_vars){
    original_type = typeof(base[[id_var]])
    id_vec = unique(as.numeric(as.factor(base[[id_var]])))
    base[!is.na(get(id_var)), (id_var) := sample(id_vec, .N, replace = T), by = dont_repeat_ids_within]
    if (original_type == 'character'){
      base[, (id_var) := as.character(get(id_var))]
    }
  }
  
  ## for each sub group generate dummy versions of remaining variables then put them all together 
  base_dummy = rbindlist(lapply(1:nrow(subsets), function(i){
    print(paste0(round(100*i / nrow(subsets),2), '%'))
    ## sample from the joint empirical distribution for discrete vars
    temp= base[subset_num == i] %>% .[,subset_num := NULL]
    temp_discrete = temp[,..full_discrete] %>% .[sample(.N, .N, replace = F)] 
    
    ## assume continuous vars are distributed truncated normal and then sample 
    if (length(continuous_vars)> 0){
    temp_continuous = do.call(cbind,lapply(continuous_vars, function(var){
      na_values = rep(NA, nrow(temp[is.na(get(var))]))
      non_na_values = na.omit(temp[[var]])
      if (length(na_values)< (nrow(temp)-1) & !is.na(sd(non_na_values))){
        if(sd(non_na_values) != 0){
          non_na_values = rtruncnorm(length(non_na_values), min(non_na_values),max(non_na_values), mean(non_na_values),sd(non_na_values))
        }
        output = data.table(var = c(na_values, non_na_values)) %>%  .[sample(.N, .N, replace = F)] 
      }else{
        output = data.table(var = na_values[1])
      }
      return(output %>% rename_with(~c(var)))}))
    
    output = cbind(temp_discrete, temp_continuous)}else{
      output = temp_discrete
    }
  }))
  if(!all_NA(rounding_vars)) base_dummy[, (rounding_vars) := lapply(rounding_vars, function(x) round(get(x)))]
  print(paste('continuous_vars are: ', paste(continuous_vars, collapse = ", ")))
  
  distinct_vars = intersect(names(base_dummy), unique(c(id_vars,dont_repeat_ids_within)))
  base_dummy = base_dummy %>% distinct(across(all_of(distinct_vars)), .keep_all = T) %>%
    select(-intersect(names(.), c('dummy', 'dummy_2')))
    
  return(base_dummy)
}


within_group_filter = function(df, condition, group_var){
  command = paste0('df[, temp :=', condition, ", by = ", group_var, '] %>% .[temp==T] %>% .[,temp:=NULL]')  
  return(eval(parse(text = command)))
}
con_fil = function(vector,...,or = T, inc = T){
  if (typeof(vector) == 'list') vector = names(vector)
  strings = c(...)
  
  if (inc){
    if(or){output = unique(unlist(lapply(strings, function(string){vector[grepl(string, vector)]})))
    }else{
      output = vector 
      for(string in strings) output = output[grepl(string, output)]
    }
  }else{
    if(or){
    output = setdiff(vector,  unique(unlist(lapply(strings, function(string){vector[grepl(string, vector)]}))))
    }else{
      output = vector 
      for(string in strings) output = output[grepl(string, output)]
      output = setdiff(vector, output)
    }
  }
  return(output)
}
sub_regression = function(...,output_model= F, predicted = F,
                          residuals = F, ssr = F, r_squared = F, 
                          asr = F){
  col_list = list(...)
  
  max_length <- max(sapply(col_list, length))
  
  # Standardize column lengths by padding with NAs
  col_list <- lapply(col_list, function(col) {
    length(col) <- max_length  # Expands or truncates the vector
    return(col)
  })
  df <- as.data.table(do.call(cbind, col_list)) 
  
  if(nrow(df %>% na.omit())<= length(col_list)) return(NA_real_) 
  command = paste0('model = lm(data = df, V1 ~',
                   gpaste('V',2:length(col_list), collapse_str = "+" ),
                   ")")
  eval(parse(text = command))
  if(r_squared) return(summary(model)[['r.squared']])
  if(output_model) return(model)
  if(ssr) return(sum(resid(model)^2))
  if(asr) return(mean(resid(model)^2))
  if(predicted){output = predict(model) %>% as.data.frame()}
  if(residuals){output = resid(model) %>% as.data.frame()}
  if(predicted | residuals){
    output = rownames_to_column(output,"index") %>% 
      mutate(index = as.numeric(index)) %>%
      merge(data.frame(index = 1:max_length), all = T) %>%
      .[,2]
    return(output)
  }
}

remove_if_NA = function(df, ...){
  var_list =  c(...)
  df = as.data.table(df)
  for (var in var_list){df = df[!is.na(get(var))]}
  return(df)
}
expand = function(..., names, order = NULL){
  input = list(...)
  if(is.null(order)){
    
    output = expand.grid(rev(input), stringsAsFactors = F) %>%
      select(rev(everything())) %>% rename_with(~names) %>% as.data.table()
  }else{
    rev_order = rep(0,length(order))
    for (i in seq_along(order)) rev_order[i] = which(order == i)[1]
    rev_order = length(input) + 1 - rev_order
    output = expand.grid(rev(input[order]), stringsAsFactors = F)
    output = output[, rev_order] %>% rename_with(~names) %>% as.data.table()
  }
  return(output)
}
index_paste = function(x, indeces, val){
  x[indeces] = paste0(x[indeces],val )
  return(x)
}


gpaste <- function(..., order = NA, collapse_str = NA, no_expand = F) {
  # Get the list of arguments as input
  args <- list(...)
  if (!no_expand){
    # Create a data frame with all combinations of the arguments
    if (any(is.na(order))){
      
      combinations <- expand.grid(rev(args), stringsAsFactors = FALSE)
      combinations = combinations[, rev(seq_along(combinations))]
    }else{
      rev_order = rep(0,length(order))
      for (i in seq_along(order)) rev_order[i] = which(order == i)[1]
      rev_order = length(args) + 1 - rev_order
      combinations = expand.grid(rev(args[order]), stringsAsFactors = F)
      combinations = combinations[, rev_order]
    }
    # Concatenate the combinations row-wise
    output <- apply(combinations, 1, paste0, collapse = "")
  }else {output = do.call(paste0, args)}
  if (!is.na(collapse_str)) output = paste(output, collapse = collapse_str)
  return(output)
}

import_file <- function(..., col_select = NULL, data_table = T, char_vars = NULL, nrows = Inf){
  filepath = paste0(...)
  import_csv = function(file, col_select = NULL, char_vars = NULL, nrows = Inf){
    if(!is.null(col_select)) col_select = paste0(", select = c('", paste(col_select, collapse = "','"), "')")
    if(!is.null(char_vars)) char_vars =  paste0(", colClasses = list(character=  c('", 
                                                paste(char_vars, collapse = "','"), "'))")
    command = paste0("fread('",file,"'", col_select, char_vars,', nrows =',nrows, ")")
    return(eval(parse(text = command)))
  }
  
  import_parquet = function(file, col_select = NULL, data_table = T){
    con = dbConnect(duckdb())
    if (!is.null(col_select)){
      file = as.data.table(dbGetQuery(con, paste0("SELECT ", paste(col_select, collapse = ", "), " FROM '",file, "'")))
    } else{
      file = as.data.table(dbGetQuery(con, paste0("SELECT * FROM '",file, "'")))
    }
    dbDisconnect(con, shutdown=TRUE); gc()
    return(file)
  }
  
  if (grepl("\\.parquet$", filepath, ignore.case = TRUE)) {
    file <- import_parquet(filepath, col_select = col_select, data_table = T) 
  } else if (grepl("\\.xlsx$|\\.xls$", filepath, ignore.case = TRUE)) {
    file <- read_excel(filepath) %>% as.data.table() 
  } else if (grepl("\\.csv$", filepath, ignore.case = TRUE)) {
    file <- import_csv(filepath, col_select = col_select, char_vars = char_vars, nrows = nrows)
  } else if (grepl("\\.rds$", filepath, ignore.case = TRUE)) {
    file <- readRDS(filepath)
  } else if(grepl("\\.shp$", filepath, ignore.case = TRUE)){
    file = st_read(filepath)
  } else if (grepl("\\.mat$", filepath, ignore.case = TRUE)){
    file = readMat(filepath)
  } else {
    stop("Unsupported file type")
  }
  if (nrows!= Inf) file = file[1:nrows]
  return(file)
}

copy_directory <- function(from_dir, to_dir) {
  # Ensure the source directory exists
  if (!dir.exists(from_dir)) {
    stop("Source directory does not exist.")
  }
  
  # Create the destination directory if it doesn't exist
  if (!dir.exists(to_dir)) {
    dir.create(to_dir, recursive = TRUE)
  }
  
  # List all files and directories (recursive)
  files <- list.files(from_dir, full.names = TRUE, recursive = TRUE)
  
  # Recreate subdirectory structure in the destination folder
  sub_dirs <- unique(dirname(files))
  sub_dirs <- gsub(from_dir, to_dir, sub_dirs)  # Adjust paths to match new root
  lapply(sub_dirs, dir.create, recursive = TRUE, showWarnings = FALSE)
  
  # Copy each file while preserving its subdirectory structure
  dest_files <- gsub(from_dir, to_dir, files)  # Adjust paths for new destination
  file.copy(files, dest_files, overwrite = TRUE)
  
  print("Directory copied successfully with all subfolders and files.")
}

exclude_from = function(base_list, exclude_elements){
  return(base_list[!base_list %in% exclude_elements])
}

replicate_var = function(data, data_dummy, var, discrete){
  if (discrete){ 
    data_dummy[[var]] = sample(exclude_from(data[[var]],NA), size = nrow(data), replace = T)
  } else{
    data$var = data[[var]] 
    data_dummy$var =  rnorm(nrow(data), mean(na.rm=T, data$var[data$var !=0 ]), sd(na.rm=T, data$var[data$var !=0 ]))
    if (NA_min(data$var) >= 0){
      data_dummy = data_dummy[var !=0, var := var - min(var,0)]
    }
    data_dummy[[var]]= data_dummy$var; data_dummy$var = NULL; data$var = NULL
  }
  return(data_dummy)
}

unbalanced_lag = function(data,id_var,time_var, value_vars, lag_amounts,
                          expand = F,expand_value = 0, birth_var =NA, death_var = NA){
  # returns a dataframe with lag and log variables that account for potentially missing observations
  # also if expand is true will set lags from before the id's birth / after it's death to zero if it lies within the 
  # time range of the dataset (otherwise will remain na)
  
  ### assign short hand versions of the variable names 
  if(('value' %in% value_vars) & length(value_vars) > 1) stop("you're about to clobber the value var; rename it")
  data = as.data.table(data)
  og_columns = names(data)
  
  final_columns = og_columns; 
  if(any( lag_amounts <0)) final_columns = c(final_columns, gpaste(value_vars,"_", paste0("lead",abs(lag_amounts %>% .[.<0]))))
  if(any( lag_amounts >0)) final_columns = c(final_columns, gpaste(value_vars,"_", paste0("lag",lag_amounts %>% .[.>0])))
  
  min_time = min(data[[time_var]]); max_time  = max(data[[time_var]]); 
  missing_time = setdiff(min_time:max_time, unique(data[[time_var]]))
  data$expand = expand
  data$time = data[[time_var]]
  data[, id := get(id_var[1])]; if (length(id_var) > 1) for (i in 2:length(id_var)){data[, id := paste(id, get(id_var[i]))] }  
  if(is.na(birth_var)){data[,birth_var := min(time), by = id_var][birth_var == min_time, birth_var :=NA]}else{data$birth_var = data[[birth_var]]}
  if(is.na(death_var)){data[,death_var := max(time), by = id_var][death_var == max_time, death_var :=NA]}else{data$death_var = data[[death_var]]}
  
  
  ### generate the lag / lead variables 
  for (value_var  in value_vars){for (lag_amount in lag_amounts){
    data[, `:=`(value = get(value_var), time_lag = get(time_var) +lag_amount)]
    
    data = merge(data, data[, .(time_lag, id, value)] %>% rename(value_lag= value),
                 by.x = c('id','time'), by.y = c('id', 'time_lag'), all.x = T) 
    
    if (lag_amount > 0) {
      data[, paste0(value_var,"_",'lag',lag_amount) := case_when(
        !is.na(value_lag) | is.na(birth_var) | !expand ~ value_lag,
        (time + 1 - lag_amount == birth_var) &! (time %in% c(missing_time + lag_amount)) ~ expand_value,
        T~ value_lag)]
      
      
    }else{
      lead_amount = -1*lag_amount
      data[, paste0(value_var,"_",'lead',lead_amount) := case_when(
        !is.na(value_lag) | is.na(death_var) | !expand ~ value_lag,
        (time -1 + lead_amount == death_var) &! (time_var %in% c(missing_time + lead_amount)) ~ expand_value,
        T~ value_lag)]
    }
    extra = setdiff(c('value','value_lag', 'time_lag'), value_var)
    data[, (extra) := NULL]
  }}
  
  return(data %>% select(all_of(final_columns)) %>% ungroup())
  
}

unbalanced_growth_rate = function(data,id_var, time_var, value_vars, time_horizon,
                                  birth_var = NA,death_var = NA, alt_suffix = NA,expand = F, keep_lags = F){
  
  og_columns = names(data)
  ## generate the initial leads and lags    
  data = unbalanced_lag(data, id_var,time_var,value_vars, lag_amounts = setdiff(-1*time_horizon,0), expand = T,
                        birth_var = birth_var, death_var = death_var)  
  lag_amount = abs(time_horizon[1]); lead_amount = time_horizon[2]
  
  ## generate the growth rates for all variables 
  suffix = ifelse(is.na(alt_suffix), '_growth_rate', alt_suffix)
  fwd = ifelse(lead_amount == 0, "",paste0('_lead', lead_amount))
  bwd =  ifelse(lag_amount == 0, "",paste0('_lag', lag_amount))
  command = paste0(suffix, " = ifelse(fwd - bwd == 0, 0, .5*(fwd - bwd)/(fwd + bwd))")
  command = lapply(value_vars, function(var){paste0(var, gsub('bwd',paste0(var, bwd),gsub('fwd', paste0(var, fwd), command)))}) %>%
    unlist() %>% paste(.,collapse = ",") %>% paste0("data = data[,`:=`(", .,")]")
  eval(parse(text = command))
  
  # drop lags if unwanted 
  final_columns = c(og_columns, paste0(value_vars, suffix))
  if(keep_lags) final_columns = names(data)
  data = data %>% select(final_columns)
}

standardize = function(x){
  x = (x - mean(x,na.rm =T)) / sd(x, na.rm = T)
}

positive_standardize = function(x){
  x = standardize(x)
  min_value = min(x, na.rm =T) 
  if (min_value < 1){
    x= x + (1 - min_value)
  }
}

generate_distribution_graphs = function(data, interest_var, granularity){
  data[, interest := get(interest_var)]
  data[, rank := cut(interest, breaks = granularity, labels = F)]
  data = data[, .(count = .N), by = rank]
  setorder(data, rank) 
  data[, cum_share := cumsum(count)/ sum(count)]
  
  PDF = ggplot(data, aes(x = rank, y =count)) + geom_point()
  CDF = ggplot(data, aes(x = rank, y = cum_share)) + geom_point()
  output = list(PDF, CDF, data)
}

pretrend_graph = function(data, x_var, group_var, legend_placement = 'bottom', 
                          subtitle = element_blank(), legend_name = element_blank(),
                          base_year_added = NA, y_label = element_blank(), x_label = element_blank()){
  data$group_var = data[[group_var]]; data$x_var = data[[x_var]]
  
  if (!is.na(base_year_added)){
    addition = data.table(x_var = base_year_added, group_var = unique(data$group_var),
                          coef = 0, ub = 0, lb = 0)
    data = rbindlist(list(data, addition), use.names = T, fill = T)
  }
  graph = ggplot(data, aes(x = x_var, y= coef, group = group_var, color = group_var)) + 
    geom_line(size = 1) + geom_ribbon(aes(ymin = lb, ymax = ub, fill = group_var), color = NA, alpha = .2) +
    guides(fill = 'none') + labs(subtitle = subtitle, x = x_label, y = y_label,
                                 color = legend_name) + theme(legend.position = legend_placement)
  return(graph)}


replace_in_files <- function(root_dir,exceptions, pattern, replacement, file_pattern = "\\.R$", checking_only = F) {
  files <- list.files(path = root_dir, pattern = file_pattern, recursive = TRUE, full.names = TRUE)
  excepted_files = con_fil(files, exceptions); print('excepted_files: '); print(excepted_files);
  files = setdiff(files,excepted_files)
  
  for (file in files) {
    content <- readLines(file, warn = FALSE)
    if (any(grepl(pattern, content, fixed = TRUE))) {
      cat("Found in: ", file, "\n")
      if (!checking_only){
      cat("Updating:", file, "\n")
      updated <- gsub(pattern, replacement, content, fixed = TRUE)
      writeLines(updated, file)
      }
    }
  }
  
}

zero_proof_ntiles = function(x, n){
  zero_indeces = x == 0
  temp = data.table(x =x[!zero_indeces]) %>% .[,ntile(x, n)]
  output = rep(0, length(x))
  output[!zero_indeces] = temp
  return(output)
}
stratified_sample = function(df, strata, sample_size){
  output = df[, .SD[sample(.N, max(1, floor(sample_size *.N)))], by = strata]
}
# na_var functions ------------------------------------------------------------------
corect_NA_type = function(column) {
  col_class <- class(column)
  
  if ("integer" %in% col_class) {
    return(NA_integer_)
  } else if ("numeric" %in% col_class) {  # includes "double"
    return(NA_real_)
  } else if ("character" %in% col_class) {
    return(NA_character_)
  } else if ("logical" %in% col_class) {
    return(NA)
  } else if ("factor" %in% col_class) {
    if(typeof(column) == 'integer') return(NA_integer_)
    if(typeof(column) == 'character') return(NA_character_)
    return(NA)
  } else if ("Date" %in% col_class) {
    return(as.Date(NA))
  } else if ("POSIXct" %in% col_class) {
    return(as.POSIXct(NA))
  } else {
    warning(paste("Unknown type:", col_class))
    return(NA)
  }
}
all_NA =  function(x){
  all(is.na(x))
}
NA_sum = function(x){
  ifelse(all_NA(x), corect_NA_type(x), sum(x,na.rm = T))
}
NA_any = function(x){
  ifelse(all_NA(x), corect_NA_type(x), any(x,na.rm = T))
}
NA_var = function(x){
  ifelse(all_NA(x), corect_NA_type(x), var(x,na.rm = T))
}
NA_first = function(x){
  ifelse(all_NA(x), corect_NA_type(x), first(x[!is.na(x)]))
}
NA_mode = function(x){
  Mode <- function(x){
    x = x[!is.na(x)]
    ux <- unique(x)
    ux[which.max(tabulate(match(x, ux)))]
  }  
  ifelse(all_NA(x), corect_NA_type(x), Mode(x))
}
NA_mean = function(x){
  ifelse(all_NA(x),NaN, mean(x,na.rm = T))
}
NA_median = function(x){
  ifelse(all_NA(x), NaN, median(x,na.rm = T))
}
NA_sd = function(x){
  ifelse(all_NA(x), corect_NA_type(x), sd(x,na.rm = T))
}
NA_max = function(x){
  ifelse(all_NA(x), corect_NA_type(x), max(x,na.rm = T))
}
NA_min = function(x){
  ifelse(all_NA(x), corect_NA_type(x), min(x,na.rm = T))
}
NA_IQR = function(x){
  ifelse(all_NA(x), corect_NA_type(x), IQR(x,na.rm = T))
}
coef_var = function(x){
  sd(x, na.rm =T) / mean(x, na.rm =T)
}
NA_coef_var = function(x){
  NA_sd(x)/NA_mean(x)
}

trend_metrics<-function(dt, vars_to_trend, vars_weight, time_var){
  setDT(dt)
  
  dt<-dt[, {
    
    res<-list()
    for(v in vars_to_trend){
      x<-get(v)
      res[[paste0(v, "_mean_unwtd")]] <- mean(x, na.rm=T)
      for(w in vars_weight){
        wt<-get(w)
        res[[paste0(v, "_mean_", w)]] <- sum(x*wt, na.rm=T)/sum(wt[!is.na(x)], na.rm=T)
      }
    }
    res
  }, by=time_var]
  
  setorder(dt, year)
  return(dt)
  
}
# core clipper
.wins <- function(v, lb, ub) {
  qs <- stats::quantile(v, probs = c(lb, ub), na.rm = TRUE, names = FALSE)
  v <- pmin(pmax(v, qs[1]), qs[2])
  v
}

# winsorize x relative to y
winsorize_relative <- function(x, y, lb = 0.01, ub = 0.99,
                               method = c("ratio", "residual", "by_bins"),
                               bins = 10) {
  method <- match.arg(method)
  stopifnot(length(x) == length(y))
  n <- length(x)
  
  # keep NA mask to put NAs back where appropriate
  na_mask <- is.na(x) | is.na(y)
  
  out <- rep(NA_real_, n)
  
  if (method == "ratio") {
    r <- x / y
    # avoid divide-by-zero/Infs
    r[!is.finite(r)] <- NA_real_
    r_w <- .wins(r, lb, ub)
    out <- r_w * y
    
  } else if (method == "residual") {
    # simple linear fit; swap to log/robust if you prefer
    fit <- stats::lm(x ~ y)
    res <- stats::residuals(fit)
    res_w <- .wins(res, lb, ub)
    out <- stats::fitted(fit) + res_w
    
  } else if (method == "by_bins") {
    # quantile bins of y, then winsorize x within each bin
    brks <- stats::quantile(y, probs = seq(0, 1, length.out = bins + 1),
                            na.rm = TRUE, names = FALSE)
    # ensure unique breakpoints (flat y edge cases)
    brks <- unique(brks)
    grp <- cut(y, breaks = brks, include.lowest = TRUE, labels = FALSE)
    out <- x
    for (g in sort(unique(grp[!is.na(grp)]))) {
      idx <- which(grp == g)
      out[idx] <- .wins(x[idx], lb, ub)
    }
  }
  
  # restore NAs where either x or y was NA
  out[na_mask] <- NA_real_
  out
}


safe_unload <- function(pkgs) {
  for (pkg in pkgs) {
    # 1) Detach from search path if attached
    if (paste0("package:", pkg) %in% search()) {
      try(detach(paste0("package:", pkg), unload = TRUE, character.only = TRUE), silent = TRUE)
    }
    # 2) If the namespace is still loaded, unload it
    if (isNamespaceLoaded(pkg)) {
      try(unloadNamespace(pkg), silent = TRUE)
    }
  }
  invisible(NULL)
}

robust_scale <- function(v){
  m <- median(v, na.rm=TRUE)
  s <- mad(v, constant = 1, na.rm=TRUE)
  if (!is.finite(s) || s == 0) s <- sd(v, na.rm=TRUE)
  (v - m) / s
}
  

  
  
  