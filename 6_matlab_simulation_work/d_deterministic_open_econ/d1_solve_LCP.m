clear all; close all; clc;
addpath(genpath('../b_helper_functions'))

% base parameters
I = 20;
num_mkts = 2;
doing_LCP =true;
% production parameters
w = 1;
phi_g = 1; % goods productivity
gamma = 4;     % CES parameter (from BEJK)
gamma_tilde = gamma/(gamma-1);
x_bar = repmat(5,1, num_mkts);
pi_bar = x_bar*w*phi_g^-1*(gamma-1)^-1; % base profits
fc = 1; fc = [fc, repmat(fc*1.3,1,num_mkts-1)];
ec = fc*2;
% data parameters
phi_d = 1; % data productivity
alpha_1 = .5; % cobb douglas coefficient on data labor
alpha_2 = .5; % cobb douglas coefficient on raw data
sigma_a = repmat(1.1,1,num_mkts); % sd of noise term
Q = zeros(num_mkts,num_mkts); lambda = .5; sigma_z = repmat(1.1,1,num_mkts);
for index = 1:num_mkts^2
    [i, j] = ind2sub([num_mkts,num_mkts], index);
    if i == j
        Q(index) = sigma_z(i)^2;
    else
        Q(index) =  sigma_z(i)*sigma_z(j)*lambda;
    end
end
D =  diag(repmat(-.9,1,num_mkts)); % mean reversion parameter term

% Simulation Parameters
rho = 0.05; %discount rate
Delta = 1000; % 1/Delta = time_step
crit = 10^(-3);
maxit = 500;

% Construct the state space
Sigma_ub = fake_layp(D, Q); % sets drift of Sigma to zero when we're in no markets
Sigma_lb = 1e-2;
state_space_ub =  Sigma_ub(triu(true(size(Sigma_ub)), 0));
vecs = arrayfun(@(x) linspace(Sigma_lb, x, I), state_space_ub, 'UniformOutput', false);
[nvecs{1:numel(vecs)}] = ndgrid(vecs{:});
Sigma = cell2mat(cellfun(@(x) x(:), nvecs, 'UniformOutput', false));
networks = [1,0;1,1];
%networks = dec2bin(0:(2^num_mkts - 1)) - '0';
num_networks = size(networks,1);
num_state_vars = size(Sigma,2);
len_Sigma = size(Sigma,1);
d_Sigma =  cellfun(@(v) v(2) - v(1), vecs);


% create a maps from state space to matrix version of sigma
index_matrix = zeros(num_mkts);
mask = triu(true(num_mkts));
index_matrix(mask) = 1:nnz(mask);
index_matrix = index_matrix + index_matrix.' - diag(diag(index_matrix));
rev_indeces= reshape(1:num_mkts^2,num_mkts, num_mkts);
rev_indeces = rev_indeces(triu(true(size(rev_indeces)), 0));
[i_idx, j_idx] = ind2sub([num_mkts, num_mkts], rev_indeces);
rev_index_matrix = [i_idx, j_idx];
Sigma_mat_version = zeros(len_Sigma,num_mkts, num_mkts);
for i = 1:num_mkts
    for j = 1:num_mkts
        Sigma_mat_version(:,i,j) =  Sigma(:,index_matrix(i,j));
    end
end
clear rev_indeces i_idx j_idx mask

% find the column of state_space that represent diagonals
diag_indeces = find(ismember(find(triu(true(num_mkts), 0)),1:num_mkts+1:num_mkts^2));
non_diag_indeces = setdiff(1:num_state_vars, diag_indeces)';
all_state_indeces = sort([diag_indeces; non_diag_indeces]);
% Construct Expected Quality / quantity
Sigma_importance = .66;
A_bar = diag(Sigma_ub).' / Sigma_importance +  sigma_a.^2 + .5*diag(Sigma_ub).';
A_tilde = A_bar - sigma_a.^2 - Sigma(:, diag_indeces);


%% complete calculations that don't require specific value functions

Sigma_comp_optimal_L = zeros([len_Sigma,repmat(num_mkts,1,3)]);
parfor j = 1:num_mkts
    for i = 1:num_mkts
        for k = 1:num_mkts
            ij = index_matrix(i, j)
            jk = index_matrix(j, k);
            Sigma_comp_optimal_L(:,i,j,k) = Sigma(:, ij) .* Sigma(:, jk);
        end
    end
end
xi = (w./(phi_d*alpha_1*(A_tilde.*x_bar).^alpha_2)).^(1/(alpha_1-1))...
    .* permute(networks, [3, 2, 1]);

% constant drift component = DSigma_t + Sigma_t + Q -jovanovich
jovanovich_drift = zeros(num_mkts,num_mkts, num_networks);
for network = 1:num_networks
    temp = repmat(sigma_a.^-2,num_networks,1).*networks;
    jovanovich_drift(:,:,network) = diag(temp(network,:));
    clear temp
end
jovanovich_drift = permute(repmat(jovanovich_drift, 1,1,1, len_Sigma), [4,1,2,3]);
jovanovich_drift = batched_matmul(...
    batched_matmul(repmat(Sigma_mat_version,[ones(1,3), num_networks]), jovanovich_drift, [2, 3, 1, 4])...
    ,repmat(Sigma_mat_version,[ones(1,3), num_networks]),[2, 3, 1, 4]);

constant_drift_component_mat = permute(repmat(...
    pagemtimes(repmat(D,1,1,len_Sigma), permute(Sigma_mat_version, [2,3,1]))...
    + pagemtimes(permute(Sigma_mat_version, [2,3,1]), repmat(D,1,1,len_Sigma))...
    + repmat(Q,1,1,len_Sigma),1,1,1,num_networks),[3,1,2,4])...
    - jovanovich_drift;
constant_drift_component = zeros(len_Sigma, num_state_vars, num_networks);
parfor state_comp = 1:num_state_vars
    i = rev_index_matrix(state_comp, 1); j = rev_index_matrix(state_comp, 2);
    constant_drift_component(:, state_comp,:) = constant_drift_component_mat(:,i,j,:);
end

% partially constant (for a particular i,k component of Sigma) with with
% respect to L_j: Sigma_ij*phi_d*(A_tilde*x_bar)^alpha_2)*Sigma_jk
% in the loop we multiply these by the L_j chosen and sum to get that
% component of drift
partially_constant_drift = zeros(len_Sigma,num_state_vars, num_mkts, num_networks);
parfor state_comp = 1:num_state_vars
    i = rev_index_matrix(state_comp, 1); k = rev_index_matrix(state_comp, 2);
    for j = 1:num_mkts
        temp = Sigma_mat_version(:,i,j)...
            .* (phi_d * (x_bar(:,j).*A_tilde(:,j)).^alpha_2)...
            .* Sigma_mat_version(:,j,k);
        temp = repmat(temp, 1,num_networks) .*repmat(networks(:,j)',len_Sigma ,1);
        partially_constant_drift(:, state_comp, j, :) = temp;
    end
end
clear jovanovich_drift constant_drift_component_mat i j k state_comp network temp

%non data-labor profit component of the hamiltonian
ham_base = squeeze(sum(repmat(A_tilde .* pi_bar -fc, [1,1,num_networks]) .* permute(networks, [3 2 1]),2));

%% Value Func Iteration
% set variables needed for inner functions
dim = struct('len_Sigma',len_Sigma,'num_mkts', num_mkts, 'num_networks', num_networks...
    , 'num_state_vars', num_state_vars, 'I', I, 'd_Sigma', d_Sigma, ...
    'index_matrix', index_matrix);

% set first guess for v --> earning profits of base state without worrying
% about data
v_0 = repmat(ham_base(len_Sigma,:),len_Sigma,1) ./ rho;
v = v_0;

for n=1:maxit
    disp(n)
    V = v;
    %Generate differences
    v_reshaped = reshape(v, [repmat(I, 1, num_state_vars), num_networks]);
    dv_f = zeros(len_Sigma, num_state_vars, num_networks); dv_b = dv_f;
    parfor state_num = 1:num_state_vars
        % Create indexing templates
        idx_all = repmat({':'}, 1, num_state_vars + 1);

        % Forward difference (except last index)
        idx_f = idx_all;        idx_f{state_num} = 1:I-1;
        idx_next = idx_all;     idx_next{state_num} = 2:I;
        temp = zeros(size(v_reshaped));
        temp(idx_f{:}) = v_reshaped(idx_next{:}) - v_reshaped(idx_f{:});

        % At boundary: use backward difference
        idx_last = idx_all;     idx_last{state_num} = I;
        idx_prev = idx_all;     idx_prev{state_num} = I-1;
        temp(idx_last{:}) = v_reshaped(idx_last{:}) - v_reshaped(idx_prev{:});

        % Store reshaped result and divide by d_Sigma
        dv_f(:, state_num, :) = reshape(temp, len_Sigma, 1, num_networks)/d_Sigma(state_num);

        % Backward difference (except first index)
        idx_b = idx_all;        idx_b{state_num} = 2:I;
        idx_prev = idx_all;     idx_prev{state_num} = 1:I-1;
        temp2 = zeros(size(v_reshaped));
        temp2(idx_b{:}) = v_reshaped(idx_b{:}) - v_reshaped(idx_prev{:});

        % At boundary: use forward difference
        idx_first = idx_all;    idx_first{state_num} = 1;
        idx_next = idx_all;     idx_next{state_num} = 2;
        temp2(idx_first{:}) = v_reshaped(idx_next{:}) - v_reshaped(idx_first{:});

        % Store reshaped result
        dv_b(:, state_num, :) = reshape(temp2, len_Sigma, 1, num_networks) / d_Sigma(state_num);
    end
    clear v_reshaped state_num

   %Carry out Updwind Procedure 
   Ib = false(len_Sigma, num_state_vars, num_networks); If = Ib;
   I_final = false(size(Ib)); 
    for i = 1:num_state_vars
        dv_b_temp = dv_b; dv_b_temp(Ib) = dv_b(Ib); dv_b_temp(If) = dv_f(If);
        dv_f_temp = dv_f; dv_f_temp(Ib) = dv_b(Ib); dv_f_temp(If) = dv_f(If);

        L_f = d1_L_optimal(dv_f_temp, dim, Sigma_comp_optimal_L,xi, alpha_1);
        L_b = d1_L_optimal(dv_b_temp, dim, Sigma_comp_optimal_L,xi, alpha_1);

        drift_f =  d1_drift_calc(L_f.^alpha_1, constant_drift_component, partially_constant_drift, dim);
        drift_b =  d1_drift_calc(L_b.^alpha_1, constant_drift_component, partially_constant_drift, dim);

        Iunique = (drift_f<0).*(drift_b<=0) | (drift_f>=0).*(drift_b>0);
        Ib = Ib | (Iunique & drift_f < 0);
        If = If | (Iunique & drift_b > 0);

       if all(Iunique(:)) || isequal(Iunique, I_final)
            break 
        else
          I_final = Ib | If;
        end
    end
    Ham_f = ham_base  - squeeze(sum(L_f.*w, 2)) + squeeze(sum(drift_f.* dv_f,2));
    Ham_b = ham_base  - squeeze(sum(L_b.*w, 2)) + squeeze(sum(drift_b.* dv_b,2));
    Ham_f = permute(repmat(Ham_f,[ones(1, 2), num_state_vars]), [1,3,2]);
    Ham_b = permute(repmat(Ham_b,[ones(1, 2), num_state_vars]), [1,3,2]);
    I0 = ~I_final & drift_f > 0;
    Ib(~I_final & ~I0) = Ham_b(~I_final & ~I0) >= Ham_f(~I_final & ~I0);
    If(~I_final & ~I0) = Ham_f(~I_final & ~I0) >  Ham_b(~I_final & ~I0);
    dv_upwind = Ib.*dv_b + If.*dv_f +.5*I0.*(dv_b +dv_f);
    
    % generate values for L, profit, and drift 
    L = d1_L_optimal(dv_upwind, dim,Sigma_comp_optimal_L,xi, alpha_1);
    pi = ham_base  - squeeze(sum(L.*w, 2));
    drift = d1_drift_calc(L.^alpha_1, constant_drift_component, partially_constant_drift, dim);

    % Solve the LCP Problem
    A_matrix = d1_construct_A_matrix(drift,dim);
    B = (rho + 1/Delta)*speye(len_Sigma.*num_networks) - A_matrix;
    

    if ~doing_LCP
        b = reshape(pi + V./Delta, [],1);
        V = reshape(B\b, [], num_networks);
    else
        pi_stacked = reshape(pi, [],1);
        v_stacked = reshape(v, [], 1);
        vstar = zeros(size(v)); best_alt = vstar;
        for network = 1:num_networks
            temp = v; temp(:,network) = -inf;
            temp = temp - sum(max(0, networks - networks(network,:)).*ec,2)'...
                -  sum(max(0, networks(network,:)- networks).*01,2)';
            [vstar(:,network), best_alt(:,network)] = max(temp, [],2);
        end
        vstar_stacked = reshape(vstar, [], 1);
        vec = pi_stacked + v_stacked/Delta;
        q = -vec + B*vstar_stacked;
        z0 = v_stacked - vstar_stacked;
        l = zeros(size(v_stacked));
        u = Inf*ones(size(v_stacked));
        z = LCP(B,q,l,u,z0,0);
        z = LCP(B,q,l,u,z,0);
        LCP_error = max(abs(z.*(B*z + q)));
        if LCP_error > 10^(-5)
            fprintf('LCP not solved, Iteration = %d\n', n);
        end
        % update the value function
        V= reshape(z+ vstar_stacked, [], num_networks);
    end
   dist(n) = max(max(abs(V - v)));
   fprintf('change = %d\n', max(max(abs(V-v))))
   fprintf('num above cutoff = %d\n', size(find(abs(V - v) > crit),1))
   v = V;
    if dist(n)<crit
        fprintf('Value Function Converged, Iteration = %d\n', n);
        break
    end
end

%% 
z_mat = reshape(z,[],num_networks);
preferred_network = repmat(1:num_networks, len_Sigma,1).*(reshape(z,[],num_networks)>0) +...
                    best_alt.*(reshape(z,[],num_networks) ==0);

time_periods = Delta*10;
state = len_Sigma*ones(time_periods,1); arrival_network = ones(time_periods, 1);
departure_network = arrival_network;
L_over_time = zeros(time_periods, num_mkts);
position = repmat(Sigma(len_Sigma, :), time_periods,1);

for t = 1:(time_periods-1)
    departure_network(t) =  preferred_network(state(t),arrival_network(t));
    L_over_time(t,:) = L(state(t),:,  departure_network(t));
    position(t+1,:) = position(t,:) + drift(state(t),:,departure_network(t))./Delta; 
    [~,min_row] =  min(sqrt(sum((Sigma - position(t+1,:)).^2, 2))); state(t+1) = min_row;
    arrival_network(t+1) = departure_network(t);
end 
%plot(1:time_periods, position(:,3))
[min(preferred_network);max(preferred_network)]