% clearvars;
dec_our_result = {};
dec_preset_result = {};

init_cache;

start_timestamp = datestr(datetime('now'));

num_sample = 50;
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
t2_dist_all = {'uniform', 'normal', 'normal1', 'normal2', 'powerlaw', 'powerlaw1', 'powerlaw2', 'uniform_max_var', 'normal_max_var', 'powerlaw_max_var'};
num_dist1 = size(t1_dist,2);
num_dist2 = 8;

dists = {};
for i = 1:num_dist1 
  for j= 1:7
    t2_dist{i, j} = t2_dist_all{j};
  end
  t2_dist{i, 8} = t2_dist_all{7 + i};
end

agg = {};
agg{1} = 'count';
agg{2} = 'sum';
agg{3} = 'avg';
% 
for k = 2
  kval = kvals(k);
  for d1 = 1:num_dist1
    for d2 = 1:num_dist2
      for p = 1:6
        for a = 1:2
          res = [];
          for s = 1:num_sample
            [actual estimate] = calculate_preset_agg(nRows, kval, t1_dist{d1}, t2_dist{d1, d2}, agg{a}, prob(p,1), prob(p,2), s);
            res(s) = estimate;
          end
          dec_preset_result{k,d1,d2,p,a} = struct;
          dec_preset_result{k,d1,d2,p,a}.nRows = nRows;
          dec_preset_result{k,d1,d2,p,a}.nKeys = kval;
          dec_preset_result{k,d1,d2,p,a}.dist1 = t1_dist{d1};
          dec_preset_result{k,d1,d2,p,a}.dist2 = t2_dist{d1, d2};
          dec_preset_result{k,d1,d2,p,a}.e1 = e1;
          dec_preset_result{k,d1,d2,p,a}.e2 = e2;
          dec_preset_result{k,d1,d2,p,a}.p1 = prob(p,1);
          dec_preset_result{k,d1,d2,p,a}.p2 = prob(p,1);
          dec_preset_result{k,d1,d2,p,a}.q1 = prob(p,2);
          dec_preset_result{k,d1,d2,p,a}.q2 = prob(p,2);
          dec_preset_result{k,d1,d2,p,a}.results = res;
          dec_preset_result{k,d1,d2,p,a}.actual = actual;
          dec_preset_result{k,d1,d2,p,a}.mean = mean(res);
          dec_preset_result{k,d1,d2,p,a}.var = var(res);
          dec_preset_result{k,d1,d2,p,a}.vmr = var(res)/mean(res);
          dec_preset_result{k,d1,d2,p,a}.smr = std(res)/mean(res);
          dec_preset_result{k,d1,d2,p,a}.estimated_var = estimate_variance(nRows, kval, t1_dist{d1}, t2_dist{d1, d2}, agg{a}, e1, e2, prob(p,1));
        end
      end
    end
  end
end
save(sprintf("./test_results/dec_preset_result - %s.mat", start_timestamp), 'dec_preset_result')

for k = 2
  kval = kvals(k);
  for d1 = 1:num_dist1
    for d2 = 1:num_dist2
      for a = 1:2
        res = [];
        for s = 1:num_sample
          [actual, estimate, p1, q1, p2, q2] = calculate_agg(nRows, kval, t1_dist{d1}, t2_dist{d1, d2}, agg{a}, s, false);
          res(s) = estimate;
        end
        dec_our_result{k, d1, d2, a} = struct;
        dec_our_result{k, d1, d2, a}.nRows = nRows;
        dec_our_result{k, d1, d2, a}.nKeys = kval;
        dec_our_result{k, d1, d2, a}.dist1 = t1_dist{d1};
        dec_our_result{k, d1, d2, a}.dist2 = t2_dist{d1, d2};
        dec_our_result{k, d1, d2, a}.e1 = e1;
        dec_our_result{k, d1, d2, a}.e2 = e2;
        dec_our_result{k, d1, d2, a}.p1 = p1;
        dec_our_result{k, d1, d2, a}.p2 = p2;
        dec_our_result{k, d1, d2, a}.q1 = q1;
        dec_our_result{k, d1, d2, a}.q2 = q2;
        dec_our_result{k, d1, d2, a}.results = res;
        dec_our_result{k, d1, d2, a}.actual = actual;
        dec_our_result{k, d1, d2, a}.mean = mean(res);
        dec_our_result{k, d1, d2, a}.var = var(res);
        dec_our_result{k, d1, d2, a}.vmr = var(res) / mean(res);
        dec_our_result{k, d1, d2, a}.smr = std(res)/mean(res);
        dec_our_result{k, d1, d2, a}.estimated_var = estimate_variance(nRows, kval, t1_dist{d1}, t2_dist{d1, d2}, agg{a}, e1, e2, p1);
      end
    end
  end
end
save(sprintf("./test_results/dec_our_result - %s.mat", start_timestamp), 'dec_our_result')
