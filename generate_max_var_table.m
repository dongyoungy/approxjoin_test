function data = generate_max_var_table(nRows, nKeys, dist)
  
  global cache;

  t1_file = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' dist '_' num2str(1) '.csv'];
  file = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' dist '_max_var_' num2str(2) '.csv']; 

  % check whether data file already exists
  if ~isfile(t1_file)
    fprintf("T1 '%s' does not exist.\n", t1_file)
    return;
  end

  if isfile(file)
    fprintf("'%s' already exists.\n", file)
    return;
  end

  if cache.isKey(t1_file) 
    T1 = cache(t1_file);
  else
    T1 = readmatrix(t1_file);
    cache(t1_file) = T1;
  end

  keys = [1:nKeys]';
  a = tabulate(T1(:,1));
  a_v = keys;
  a_v(a(:,1), 2) = a(a(:,1), 2);
  a_v(:,3) = a_v(:,2) .^ 2;

  [max_val, max_key] = max(a_v(:,2))
  idx = randi(nKeys, nRows, 1);

  % force 3/4 of the keys to be max_key
  max_idx = randperm(nRows, round(0.75 * nRows));
  idx(max_idx) = max_key;
  
  % create joinkey_col1_col2 template first
  % fixed to not use template since having key -> value depedency makes
  % sum, avg cases too obvious
  value1 = round(normrnd(100, 25, [nRows, 1]),1);
  value2 = randi(100, nRows, 1);
%   records = [records value1 value2];   
  
  data = [];
  data(:,1) = idx;
  data(:,2) = value1;
  data(:,3) = value2;
  
  writematrix(data, file);  
  return;
end
