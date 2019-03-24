% temp script file to run other scripts

% generate_table_data(10000000, 100000, 'uniform', 1);
% generate_table_data(10000000, 100000, 'uniform', 2);
% generate_table_data(10000000, 100000, 'normal', 1);
% generate_table_data(10000000, 100000, 'normal', 2);
% generate_table_data(10000000, 100000, 'powerlaw', 1);
% generate_table_data(10000000, 100000, 'powerlaw', 2);
% 
% generate_table_data(10000000, 10000000, 'uniform', 1);
% generate_table_data(10000000, 10000000, 'uniform', 2);
% generate_table_data(10000000, 10000000, 'normal', 1);
% generate_table_data(10000000, 10000000, 'normal', 2);
% generate_table_data(10000000, 10000000, 'powerlaw', 1);
% generate_table_data(10000000, 10000000, 'powerlaw', 2);

% generate_sample_pair(10000000, 10000, 'uniform', 'uniform', 'count', 1)
% generate_sample_pair(10000000, 10000, 'uniform', 'normal', 'count', 1)
% generate_sample_pair(10000000, 10000, 'uniform', 'powerlaw', 'count', 1)
% generate_sample_pair(10000000, 10000, 'normal', 'normal', 'count', 1)
% generate_sample_pair(10000000, 10000, 'normal', 'powerlaw', 'count', 1)
% generate_sample_pair(10000000, 10000, 'powerlaw', 'powerlaw', 'count', 1)
% 
% generate_sample_pair(10000000, 10000000, 'uniform', 'uniform', 'count', 1)
% generate_sample_pair(10000000, 10000000, 'uniform', 'normal', 'count', 1)
% generate_sample_pair(10000000, 10000000, 'uniform', 'powerlaw', 'count', 1)
% generate_sample_pair(10000000, 10000000, 'normal', 'normal', 'count', 1)
% generate_sample_pair(10000000, 10000000, 'normal', 'powerlaw', 'count', 1)
% generate_sample_pair(10000000, 10000000, 'powerlaw', 'powerlaw', 'count', 1)
% for k = [10000 100000 1000000 10000000]
  for k = [100000 1000000 10000000]
    for sample_id = 2:40
      
%   calculate_agg(10000000, k, 'uniform', 'uniform', 'count', 1);
%   calculate_agg(10000000, k, 'uniform', 'normal', 'count', 1);
%   calculate_agg(10000000, k, 'uniform', 'powerlaw', 'count', 1);
%   calculate_agg(10000000, k, 'normal', 'normal', 'count', 1);
%   calculate_agg(10000000, k, 'normal', 'powerlaw', 'count', 1);
%   calculate_agg(10000000, k, 'powerlaw', 'powerlaw', 'count', 1);
%       generate_sample_pair(10000000, k, 'uniform', 'uniform', 'count', sample_id);
%       generate_sample_pair(10000000, k, 'uniform', 'normal', 'count', sample_id);
%       generate_sample_pair(10000000, k, 'uniform', 'powerlaw', 'count', sample_id);
%       generate_sample_pair(10000000, k, 'normal', 'normal', 'count', sample_id);
%       generate_sample_pair(10000000, k, 'normal', 'powerlaw', 'count', sample_id);
%       generate_sample_pair(10000000, k, 'powerlaw', 'powerlaw', 'count', sample_id);
    end
  end
  
  e1 = [];
  e2 = [];
  e3 = [];
  e4 = [];
  e5 = [];
  e6 = [];
  k_val = [100000 1000000 10000000]
  for k_idx = 3
    k = k_val(k_idx);
    for sample_id = 1
      [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'uniform', 'count', sample_id);
      a1(k_idx) = a;
      e1(k_idx, sample_id) = e;
      [p1,q1,p2,q2]
      
      [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'normal', 'count', sample_id);
      a2(k_idx) = a;
      e2(k_idx, sample_id) = e;
      [p1,q1,p2,q2]
      
      [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'uniform', 'powerlaw', 'count', sample_id);
      a3(k_idx) = a;
      e3(k_idx, sample_id) = e;
      [p1,q1,p2,q2]
      
      [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'normal', 'count', sample_id);
      a4(k_idx) = a;
      e4(k_idx, sample_id) = e;
      [p1,q1,p2,q2]
      
      [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'normal', 'powerlaw', 'count', sample_id);
      a5(k_idx) = a;
      e5(k_idx, sample_id) = e;
      [p1,q1,p2,q2]
      
      [a,e,p1,q1,p2,q2] = calculate_agg(10000000, k, 'powerlaw', 'powerlaw', 'count', sample_id);
      a6(k_idx) = a;
      e6(k_idx, sample_id) = e;
      [p1,q1,p2,q2]
    end
  end
  