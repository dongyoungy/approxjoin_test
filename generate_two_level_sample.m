function generate_two_level_sample(nRows, nKeys, leftDist, rightDist, aggFunc, sampleIdx)

  global cache;
  
  leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
  rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];
  
  leftSample = ['./two_level_sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '_s1_' num2str(sampleIdx) '.mat'];
  rightSample = ['./two_level_sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '_s2_' num2str(sampleIdx) '.mat'];
  
  % check original datafile
  if ~isfile(leftFile)
    fprintf("Data file do not exist: %s", leftFile)
    return;
  end
  if ~isfile(rightFile)
    fprintf("Data file do not exist: %s", rightFile)
    return;
  end
  
  % check whether sample file already exists
  if isfile(leftSample) && isfile(rightSample)
    fprintf("samples already exists.\n")
    return;
  end
  
  if cache.isKey(leftFile) 
    T1 = cache(leftFile);
  else
    T1 = readmatrix(leftFile);
    cache(leftFile) = T1;
  end
  
  if cache.isKey(rightFile) 
    T2 = cache(rightFile);
  else
    T2 = readmatrix(rightFile);
    cache(rightFile) = T2;
  end

  keys = [1:nKeys]';

  % build a table for a_v,b_v,a_v^2,b_v^2
  a = tabulate(T1(:,1));
  a_v = keys;
  a_v(a(:,1), 2) = a(a(:,1), 2);
  a_v(:,3) = a_v(:,2) .^ 2;
  
  b = tabulate(T2(:,1));
  b_v = keys;
  b_v(b(:,1), 2) = b(b(:,1), 2);
  b_v(:,3) = b_v(:,2) .^ 2;


  sigma1 = @(v, a_v, b_v, q) ((1/q^2) - 1) * (a_v(v,2) - 1) * (b_v(v,2) - 1) + (1/q - 1) * (b_v(v,2)-1) * (a_v(v,3) - a_v(v,2) + 1) * (1/q - 1) * (a_v(v,2) - 1) * (b_v(v,3) - b_v(v,2) + 1);
  fn = @(v, a_v, b_v, q) sum( sqrt ( (sigma1(v, a_v, b_v, q) + a_v(v,3) * b_v(v,3)) * (2 + q * (a_v(v,2) + b_v(v,2) - 2))  ) )^2