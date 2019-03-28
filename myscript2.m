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

nRows = 10000000;
kvals = [10000000 1000000 100000];

budget = 0.01;

% for k=1
%   nKeys = kvals(k);
%   for d=[1 2 4 6]
%     for s=1:20
%       generate_two_level_sample(nRows, nKeys, dist{d,1}, dist{d,2}, budget, s);
%     end    
%   end
% end

two_level_results = {};
for k=1
  nKeys = kvals(k);
  for d=[1 2 4 6]
    res = [];
    for s=1:20  
      [a e] = calculate_two_level_count(nRows, nKeys, dist{d,1}, dist{d,2}, s);
      res(s) = e;
    end    
    two_level_results{d} = res;
  end
end