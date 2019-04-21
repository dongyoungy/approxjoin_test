% clearvars;
our_result = {};
preset_result = {};

init_cache;

num_sample = 100;
nRows = 10000000;
kvals = [10000000 1000000];

e1 = 0.01; e2 = 0.01;

prob = []; % p,q pair
prob(1,1:2) = [0.01 1];
prob(2,1:2) = [0.015 0.666];
prob(3,1:2) = [0.03 0.333];
prob(4,1:2) = [0.333 0.03];
prob(5,1:2) = [0.666 0.015];
prob(6,1:2) = [1 0.01];

t1_dist = {'uniform', 'normal', 'powerlaw'};
t2_dist = {'uniform', 'normal', 'normal1', 'normal2', 'powerlaw', 'powerlaw1', 'powerlaw2', 'uniform_max_var', 'normal_max_var', 'powerlaw_max_var'};
num_t2_dist = size(t2_dist, 2);

dist = {};

dist{1,1} = 'uniform'; dist{1,2} = 'uniform';
dist{2,1} = 'uniform'; dist{2,2} = 'normal';
dist{3,1} = 'uniform'; dist{3,2} = 'powerlaw';
dist{4,1} = 'normal'; dist{4,2} = 'normal';
dist{5,1} = 'normal'; dist{5,2} = 'powerlaw';
% dist{6,1} = 'powerlaw'; dist{6,2} = 'powerlaw';
% dist{7,1} = 'uniform'; dist{7,2} = 'uniform_max_var';
% dist{8,1} = 'normal'; dist{8,2} = 'normal_max_var';
% dist{9,1} = 'powerlaw'; dist{9,2} = 'powerlaw_max_var';
num_dist = 5;

% k=1;
% for i = 1:t1_dist
  % for j = 1:t2_dist
    % dist{k, 1} = t1_dist{i};
    % dist{k, 2} = t2_dist{j};
    % k = k + 1;
  % end
% end
% num_dist = k;

agg = {};
agg{1} = 'count';
agg{2} = 'sum';
agg{3} = 'avg';
% 
for k = 1:1
  kval = kvals(k);
  for d = 1:num_dist
    for p = 1:6
      for a = 1:3
        res = [];
        for s = 1:num_sample
          [actual estimate] = calculate_preset_agg(nRows, kval, dist{d,1}, dist{d,2}, agg{a}, prob(p,1), prob(p,2), s);
          res(s) = estimate;
        end
        preset_result{k,d,p,a} = struct;
        preset_result{k,d,p,a}.nRows = nRows;
        preset_result{k,d,p,a}.nKeys = kval;
        preset_result{k,d,p,a}.dist1 = dist{d,1};
        preset_result{k,d,p,a}.dist2 = dist{d,2};
        preset_result{k,d,p,a}.e1 = e1;
        preset_result{k,d,p,a}.e2 = e2;
        preset_result{k,d,p,a}.p1 = prob(p,1);
        preset_result{k,d,p,a}.p2 = prob(p,1);
        preset_result{k,d,p,a}.q1 = prob(p,2);
        preset_result{k,d,p,a}.q2 = prob(p,2);
        preset_result{k,d,p,a}.results = res;
        preset_result{k,d,p,a}.actual = actual;
        preset_result{k,d,p,a}.mean = mean(res);
        preset_result{k,d,p,a}.var = var(res);
        preset_result{k,d,p,a}.vmr = var(res)/mean(res);
        preset_result{k,d,p,a}.smr = std(res)/mean(res);
        preset_result{k,d,p,a}.estimated_var = estimate_variance(nRows, kval, dist{d,1}, dist{d,2}, agg{a}, e1, e2, prob(p,1));
      end
    end
  end
end
save(sprintf("./test_results/preset_result - %s.mat", datestr(datetime('now'))), 'preset_result')

for k = 1:1
  kval = kvals(k);
  for d = 1:num_dist
    for a = 1:3
      res = [];
      for s = 1:num_sample
        [actual, estimate, p1, q1, p2, q2] = calculate_agg(nRows, kval, dist{d, 1}, dist{d, 2}, agg{a}, s);
        res(s) = estimate;
      end
      our_result{k, d, a} = struct;
      our_result{k, d, a}.nRows = nRows;
      our_result{k, d, a}.nKeys = kval;
      our_result{k, d, a}.dist1 = dist{d, 1};
      our_result{k, d, a}.dist2 = dist{d, 2};
      our_result{k, d, a}.e1 = e1;
      our_result{k, d, a}.e2 = e2;
      our_result{k, d, a}.p1 = p1;
      our_result{k, d, a}.p2 = p2;
      our_result{k, d, a}.q1 = q1;
      our_result{k, d, a}.q2 = q2;
      our_result{k, d, a}.results = res;
      our_result{k, d, a}.actual = actual;
      our_result{k, d, a}.mean = mean(res);
      our_result{k, d, a}.var = var(res);
      our_result{k, d, a}.vmr = var(res) / mean(res);
      our_result{k, d, a}.smr = std(res)/mean(res);
      our_result{k, d, a}.estimated_var = estimate_variance(nRows, kval, dist{d, 1}, dist{d, 2}, agg{a}, e1, e2, p1);
    end
  end
end
save(sprintf("./test_results/our_result - %s.mat", datestr(datetime('now'))), 'our_result')
