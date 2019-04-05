% temp script file to run other scripts

% generate_table_data(10000000, 100000, 'uniform', 1);
% generate_table_data(10000000, 100000, 'uniform', 2);
% generate_table_data(10000000, 100000, 'normal', 1);
% generate_table_data(10000000, 100000, 'normal', 2);
% generate_table_data(10000000, 100000, 'powerlaw', 1);
% generate_table_data(10000000, 100000, 'powerlaw', 2);
% 

% for k = [100000 1000000 10000000]
%   generate_table_data(10000000, k, 'uniform', 1);
%   generate_table_data(10000000, k, 'uniform', 2);
%   generate_table_data(10000000, k, 'normal', 1);
%   generate_table_data(10000000, k, 'normal', 2);
%   generate_table_data(10000000, k, 'powerlaw', 1);
%   generate_table_data(10000000, k, 'powerlaw', 2);
% end



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

% k = 10000000;
% calculate_agg(10000000, k, 'uniform', 'uniform', 'count', 1);
% calculate_agg(10000000, k, 'uniform', 'normal', 'count', 1);
% calculate_agg(10000000, k, 'uniform', 'powerlaw', 'count', 1);
% calculate_agg(10000000, k, 'normal', 'normal', 'count', 1);
% calculate_agg(10000000, k, 'normal', 'powerlaw', 'count', 1);
% calculate_agg(10000000, k, 'powerlaw', 'powerlaw', 'count', 1);

for k = [1000000]
  for sample_id = 1:100
    generate_sample_pair(10000000, k, 'uniform', 'uniform', 'count', sample_id, false);
    generate_sample_pair(10000000, k, 'uniform', 'normal', 'count', sample_id, false);
    generate_sample_pair(10000000, k, 'uniform', 'powerlaw', 'count', sample_id, false);
    generate_sample_pair(10000000, k, 'normal', 'normal', 'count', sample_id, false);
    generate_sample_pair(10000000, k, 'normal', 'powerlaw', 'count', sample_id, false);
    generate_sample_pair(10000000, k, 'powerlaw', 'powerlaw', 'count', sample_id, false);

    generate_sample_pair(10000000, k, 'normal', 'uniform', 'count', sample_id, false);
    generate_sample_pair(10000000, k, 'powerlaw', 'uniform', 'count', sample_id, false);
    generate_sample_pair(10000000, k, 'powerlaw', 'normal', 'count', sample_id, false);
    
    %%%% # rows = 10M, # keys = 10M, {Normal, Powerlaw}

    % generate_sample_pair(10000000, k, 'uniform', 'uniform', 'sum', sample_id);
    % generate_sample_pair(10000000, k, 'uniform', 'normal', 'sum', sample_id);
    % generate_sample_pair(10000000, k, 'uniform', 'powerlaw', 'sum', sample_id);
    % generate_sample_pair(10000000, k, 'normal', 'normal', 'sum', sample_id);
    % generate_sample_pair(10000000, k, 'normal', 'powerlaw', 'sum', sample_id);
    % generate_sample_pair(10000000, k, 'powerlaw', 'powerlaw', 'sum', sample_id);
    
    % generate_sample_pair(10000000, k, 'uniform', 'uniform', 'avg', sample_id);
    % generate_sample_pair(10000000, k, 'uniform', 'normal', 'avg', sample_id);
    % generate_sample_pair(10000000, k, 'uniform', 'powerlaw', 'avg', sample_id);
    % generate_sample_pair(10000000, k, 'normal', 'normal', 'avg', sample_id);
    % generate_sample_pair(10000000, k, 'normal', 'powerlaw', 'avg', sample_id);
    % generate_sample_pair(10000000, k, 'powerlaw', 'powerlaw', 'avg', sample_id);
  end
end

prob = []; % p,q pair
prob(1,1:2) = [0.01 1];
prob(2,1:2) = [0.015 0.666];
prob(3,1:2) = [0.03 0.333];
prob(4,1:2) = [0.333 0.03];
prob(5,1:2) = [0.666 0.015];
prob(6,1:2) = [1 0.01];

dist = {};
dist{1,1} = 'uniform'; dist{1,2} = 'uniform';
dist{2,1} = 'uniform'; dist{2,2} = 'normal';
dist{3,1} = 'uniform'; dist{3,2} = 'powerlaw';
dist{4,1} = 'normal'; dist{4,2} = 'normal';
dist{5,1} = 'normal'; dist{5,2} = 'powerlaw';
dist{6,1} = 'powerlaw'; dist{6,2} = 'powerlaw';

agg = {};
agg{1} = 'count';
agg{2} = 'sum';
agg{3} = 'avg';

% for k = [10000000 1000000 100000]
% for k = 10000000
%   for p = 1:6
%     for s = 1:100
%       generate_preset_sample_pair(10000000, k, 'uniform', 'uniform', prob(p,1), prob(p,2), s); 
%       generate_preset_sample_pair(10000000, k, 'uniform', 'normal', prob(p,1), prob(p,2), s); 
%       generate_preset_sample_pair(10000000, k, 'uniform', 'powerlaw', prob(p,1), prob(p,2), s); 
%       generate_preset_sample_pair(10000000, k, 'normal', 'normal', prob(p,1), prob(p,2), s); 
%       generate_preset_sample_pair(10000000, k, 'normal', 'powerlaw', prob(p,1), prob(p,2), s); 
%       generate_preset_sample_pair(10000000, k, 'powerlaw', 'powerlaw', prob(p,1), prob(p,2), s); 
%     end   
%   end
% end

kvals = [10000000 1000000 100000];
% preset_result = {};
% for k = 1:3
%   kval = kvals(k);
%   for d = 1:6
%     for p = 1:6
%       for a = 1:3
%         res = [];
%         for s = 1:20
%           [actual estimate] = calculate_preset_agg(10000000, kval, dist{d,1}, dist{d,2}, agg{a}, prob(p,1), prob(p,2), s);
%           res(s) = estimate;
%         end
%         preset_result{k,d,p,a} = res;
%       end
%     end
%   end
% end

% for k = 1
%   kval = kvals(k);
%   for d = 6
%     for p = 1:6
%       for a = 1
%         res = [];
%         for s = 1:100
%           [actual estimate] = calculate_preset_agg(10000000, kval, dist{d,1}, dist{d,2}, agg{a}, prob(p,1), prob(p,2), s);
%           res(s) = estimate;
%         end
%         preset_result{k,d,p,a} = res;
%       end
%     end
%   end
% end
