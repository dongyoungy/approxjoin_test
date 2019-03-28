k_val = [100000 1000000 10000000];

p1_count = {};
p2_count = {};
p3_count = {};
p4_count = {};
p5_count = {};
p6_count = {};

p1_sum = {};
p2_sum = {};
p3_sum = {};
p4_sum = {};
p5_sum = {};
p6_sum = {};

p1_avg = {};
p2_avg = {};
p3_avg = {};
p4_avg = {};
p5_avg = {};
p6_avg = {};
% 
% for k_idx = 3
%   k = k_val(k_idx);
%   for sample_id = 1:100
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'uniform', 'count', sample_id);
    % a1_count(k_idx) = a;
    % e1_count(k_idx, sample_id) = e;
    % p1_count{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'normal', 'count', sample_id);
    % a2_count(k_idx) = a;
    % e2_count(k_idx, sample_id) = e;
    % p2_count{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'powerlaw', 'count', sample_id);
    % a3_count(k_idx) = a;
    % e3_count(k_idx, sample_id) = e;
    % p3_count{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'normal', 'count', sample_id);
    % a4_count(k_idx) = a;
    % e4_count(k_idx, sample_id) = e;
    % p4_count{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'powerlaw', 'count', sample_id);
    % a5_count(k_idx) = a;
    % e5_count(k_idx, sample_id) = e;
    % p5_count{k_idx} = [p1,q1,p2,q2];
    
%     [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'powerlaw', 'powerlaw', 'count', sample_id);
%     a6_count(k_idx) = a;
%     e6_count(k_idx, sample_id) = e;
%     p6_count{k_idx} = [p1,q1,p2,q2];
%   end
% end


% for k_idx = 3
%   k = k_val(k_idx);
%   for sample_id = 1:100
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'uniform', 'sum', sample_id);
    % a1_sum(k_idx) = a;
    % e1_sum(k_idx, sample_id) = e;
    % p1_sum{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'normal', 'sum', sample_id);
    % a2_sum(k_idx) = a;
    % e2_sum(k_idx, sample_id) = e;
    % p2_sum{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'powerlaw', 'sum', sample_id);
    % a3_sum(k_idx) = a;
    % e3_sum(k_idx, sample_id) = e;
    % p3_sum{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'normal', 'sum', sample_id);
    % a4_sum(k_idx) = a;
    % e4_sum(k_idx, sample_id) = e;
    % p4_sum{k_idx} = [p1,q1,p2,q2];
    
%     [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'powerlaw', 'sum', sample_id);
%     a5_sum(k_idx) = a;
%     e5_sum(k_idx, sample_id) = e;
%     p5_sum{k_idx} = [p1,q1,p2,q2];
    
    % [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'powerlaw', 'powerlaw', 'sum', sample_id);
    % a6_sum(k_idx) = a;
    % e6_sum(k_idx, sample_id) = e;
    % p6_sum{k_idx} = [p1,q1,p2,q2];
%   end
% end

for k_idx = 1:3
  k = k_val(k_idx);
  for sample_id = 1:20
    [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'uniform', 'avg', sample_id);
    a1_avg(k_idx) = a;
    e1_avg(k_idx, sample_id) = e;
    p1_avg{k_idx} = [p1,q1,p2,q2];
    
    [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'normal', 'avg', sample_id);
    a2_avg(k_idx) = a;
    e2_avg(k_idx, sample_id) = e;
    p2_avg{k_idx} = [p1,q1,p2,q2];
    
    [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'powerlaw', 'avg', sample_id);
    a3_avg(k_idx) = a;
    e3_avg(k_idx, sample_id) = e;
    p3_avg{k_idx} = [p1,q1,p2,q2];
    
    [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'normal', 'avg', sample_id);
    a4_avg(k_idx) = a;
    e4_avg(k_idx, sample_id) = e;
    p4_avg{k_idx} = [p1,q1,p2,q2];
    
    [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'powerlaw', 'avg', sample_id);
    a5_avg(k_idx) = a;
    e5_avg(k_idx, sample_id) = e;
    p5_avg{k_idx} = [p1,q1,p2,q2];
    
    [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'powerlaw', 'powerlaw', 'avg', sample_id);
    a6_avg(k_idx) = a;
    e6_avg(k_idx, sample_id) = e;
    p6_avg{k_idx} = [p1,q1,p2,q2];
  end
end