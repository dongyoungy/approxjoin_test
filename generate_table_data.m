function data = generate_table_data(nRows, nKeys, dist, n)
  
  file = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' dist '_' num2str(n) '.csv'];
  
  % check whether data file already exists
  if isfile(file)
    fprintf("'%s' already exists.\n", file)
    return;
  end
  
  idx = [];
  if strcmp(dist, 'uniform')
    idx = randi(nKeys, nRows, 1);
  elseif strcmp(dist, 'normal')
    idx = round(nKeys/2+TruncatedGaussian(nKeys/5,[1 nKeys]-(nKeys/2),[1 nRows]))';
  elseif strcmp(dist, 'powerlaw')
    alpha = -2.5;
    minv = 1;
    maxv = nKeys;
    idx = floor(((maxv^(alpha+1) - minv^(alpha+1))* rand(nRows, 1) + minv.^(alpha+1)).^(1/(alpha+1)));
  else
    fprintf("unknown dist: %s", dist)
    return;
  end
  
  % create joinkey_col1_col2 template first
  % fixed to not use template since having key -> value depedency makes
  % sum, avg cases too obvious
  value1 = round(normrnd(100, 25, [nRows, 1]),1);
  value2 = randi(100, nRows, 1);
%   records = [records value1 value2];   

  % randomize keys
  keyPermMap = [1:nKeys]';
  keyPermMap(:,2) = randperm(nKeys)';
  
  data = [];
%   data(:,1) = records(idx, 1);
  data(:,1) = keyPermMap(idx(:,1), 2);
  data(:,2) = value1;
  data(:,3) = value2;
  
  writematrix(data, file);  
  return;
end
