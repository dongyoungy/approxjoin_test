% clearvars;
% our_result = {};
% dec_our_result = {};
% preset_result = {};

% init_cache;

num_sample = 100;
nRows = 10000000;
kvals = [10000000 1000000 100000];

e1 = 0.01; e2 = 0.01;

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

% for k = 2
%   kval = kvals(k);
%   for d = 1:6
%     for p = 1:6
%       for a = 1:3
%         res = [];
%         for s = 1:num_sample
%           [actual estimate] = calculate_preset_agg(nRows, kval, dist{d,1}, dist{d,2}, agg{a}, prob(p,1), prob(p,2), s);
%           res(s) = estimate;
%         end
%         preset_result{k,d,p,a} = struct;
%         preset_result{k,d,p,a}.nRows = nRows;
%         preset_result{k,d,p,a}.nKeys = kval;
%         preset_result{k,d,p,a}.dist1 = dist{d,1};
%         preset_result{k,d,p,a}.dist2 = dist{d,2};
%         preset_result{k,d,p,a}.e1 = e1;
%         preset_result{k,d,p,a}.e2 = e2;
%         preset_result{k,d,p,a}.p1 = prob(p,1);
%         preset_result{k,d,p,a}.p2 = prob(p,1);
%         preset_result{k,d,p,a}.q1 = prob(p,2);
%         preset_result{k,d,p,a}.q2 = prob(p,2);
%         preset_result{k,d,p,a}.results = res;
%         preset_result{k,d,p,a}.actual = actual;
%         preset_result{k,d,p,a}.mean = mean(res);
%         preset_result{k,d,p,a}.var = var(res);
%         preset_result{k,d,p,a}.vmr = var(res)/mean(res);
%         preset_result{k,d,p,a}.estimated_var = estimate_variance(nRows, kval, dist{d,1}, dist{d,2}, agg{a}, e1, e2, prob(p,1));
%       end
%     end
%   end
% end
% save(sprintf("./test_results/preset_result - %s.mat", datestr(datetime('now'))), 'preset_result')

% for k = 2
%   kval = kvals(k);
%   for d = 1:6
%     for a = 1:3
%       res = [];
%       for s = 1:num_sample
%         [actual, estimate, p1, q1, p2, q2] = calculate_agg(nRows, kval, dist{d, 1}, dist{d, 2}, agg{a}, s);
%         res(s) = estimate;
%       end
%       our_result{k, d, a} = struct;
%       our_result{k, d, a}.nRows = nRows;
%       our_result{k, d, a}.nKeys = kval;
%       our_result{k, d, a}.dist1 = dist{d, 1};
%       our_result{k, d, a}.dist2 = dist{d, 2};
%       our_result{k, d, a}.e1 = e1;
%       our_result{k, d, a}.e2 = e2;
%       our_result{k, d, a}.p1 = p1;
%       our_result{k, d, a}.p2 = p2;
%       our_result{k, d, a}.q1 = q1;
%       our_result{k, d, a}.q2 = q2;
%       our_result{k, d, a}.results = res;
%       our_result{k, d, a}.actual = actual;
%       our_result{k, d, a}.mean = mean(res);
%       our_result{k, d, a}.var = var(res);
%       our_result{k, d, a}.vmr = var(res) / mean(res);
%       our_result{k, d, a}.estimated_var = estimate_variance(nRows, kval, dist{d, 1}, dist{d, 2}, agg{a}, e1, e2, p1);
%     end
%   end
% end
% save(sprintf("./test_results/our_result - %s.mat", datestr(datetime('now'))), 'our_result')

for k = 1
  kval = kvals(k);
  for d = 1:6
    for a = 1
      for l = 1:2
      if l == 1
        useLeft = true;
      else
        useLeft = false;
      end  
      res = [];
      for s = 1:num_sample
        [actual, estimate, p1, q1, p2, q2] = calculate_agg(nRows, kval, dist{d, 1}, dist{d, 2}, agg{a}, s, false, useLeft);
        res(s) = estimate;
      end
      dec_our_result{k, d, a, l} = struct;
      dec_our_result{k, d, a, l}.nRows = nRows;
      dec_our_result{k, d, a, l}.nKeys = kval;
      dec_our_result{k, d, a, l}.dist1 = dist{d, 1};
      dec_our_result{k, d, a, l}.dist2 = dist{d, 2};
      dec_our_result{k, d, a, l}.e1 = e1;
      dec_our_result{k, d, a, l}.e2 = e2;
      dec_our_result{k, d, a, l}.p1 = p1;
      dec_our_result{k, d, a, l}.p2 = p2;
      dec_our_result{k, d, a, l}.q1 = q1;
      dec_our_result{k, d, a, l}.q2 = q2;
      dec_our_result{k, d, a, l}.results = res;
      dec_our_result{k, d, a, l}.actual = actual;
      dec_our_result{k, d, a, l}.mean = mean(res);
      dec_our_result{k, d, a, l}.var = var(res);
      dec_our_result{k, d, a, l}.vmr = var(res) / mean(res);
      dec_our_result{k, d, a, l}.estimated_var = estimate_variance(nRows, kval, dist{d, 1}, dist{d, 2}, agg{a}, e1, e2, p1);
      end
    end
  end
end
save(sprintf("./test_results/dec_our_result - %s.mat", datestr(datetime('now'))), 'dec_our_result')
