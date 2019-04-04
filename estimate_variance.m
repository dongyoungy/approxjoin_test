function estimate = estimate_variance(nRows, nKeys, leftDist, rightDist, aggFunc, e1, e2, p)

  global cache;
  
  leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
  rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];

  % check original datafile
  if ~isfile(leftFile)
    fprintf("Data file do not exist: %s", leftFile)
    return;
  end
  if ~isfile(rightFile)
    fprintf("Data file do not exist: %s", rightFile)
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

  % sampling budget
  keys = [1:nKeys]';

  key_a = [leftFile '_a_v'];
  key_b = [rightFile '_b_v'];

  if cache.isKey(key_a)
    a_v = cache(key_a);
  else
    a = tabulate(T1(:,1));
    a_v = keys;
    a_v(a(:,1), 2) = a(a(:,1), 2);
    a_v(:,3) = a_v(:,2) .^ 2;
    cache(key_a) = a_v;
  end

  if cache.isKey(key_b)
    b_v = cache(key_b);
  else
    b = tabulate(T2(:,1));
    b_v = keys;
    b_v(b(:,1), 2) = b(b(:,1), 2);
    b_v(:,3) = b_v(:,2) .^ 2;
    cache(key_b) = b_v;
  end

  estimate = 0;

  q1 = e1 / p;
  q2 = e2 / p;

  if strcmp(aggFunc, 'count')
    % calculate theoretical estimate
    v1 = (1/p - 1) * sum(a_v(:,3) .* b_v(:,3));
    v2 = (1/e2 - 1/p) * sum(a_v(:,3) .* b_v(:,2));
    v3 = (1/e1 - 1/p) * sum(a_v(:,2) .* b_v(:,3));
    v4 = (p/(e1*e2) - 1/e1 - 1/e2 - 1/p) * sum(a_v(:,2) .* b_v(:,2));

    estimate = v1 + v2 + v3 + v4;
  elseif strcmp(aggFunc, 'sum')
    mu_v = [1:nKeys]';
    var_v = [1:nKeys]';
    
    key_mean = [leftFile '_mean'];
    key_var = [leftFile '_var'];
    key_gname = [leftFile '_gname'];
    
    if cache.isKey(key_mean) && cache.isKey(key_var) && cache.isKey(key_gname)
      means = cache(key_mean);
      vars = cache(key_var);
      grps = cache(key_gname);
    else
      [means vars grps] = grpstats(T1(:,2), T1(:,1), {'mean', 'var', 'gname'});
      cache(key_mean) = means;
      cache(key_var) = vars;
      cache(key_gname) = grps;
    end
   
    grps = str2num(char(grps{:,1}));
    mu_v(grps, 2) = means;
    mu_v(:,3) = mu_v(:,2) .^ 2;
    var_v(grps, 2) = vars;

    % calculate theoretical estimate
    v1 = (1/e2 - 1/p) * sum(a_v(:,3) .* mu_v(:,3) .* b_v(:,2));
    v2 = (1/e1 - 1/p) * sum(a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,3));
    v3 = (p/(e1*e2) - 1/e1 - 1/e2 + 1/p) * sum(a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,2));
    v4 = (1/p - 1) * sum(a_v(:,3) .* mu_v(:,3) .* b_v(:,3));

    estimate = v1 + v2 + v3 + v4;
  elseif strcmp(aggFunc, 'avg')
    mu_v = [1:nKeys]';
    var_v = [1:nKeys]';
    
    key_mean = [leftFile '_mean'];
    key_var = [leftFile '_var'];
    key_gname = [leftFile '_gname'];
    
    if cache.isKey(key_mean) && cache.isKey(key_var) && cache.isKey(key_gname)
      means = cache(key_mean);
      vars = cache(key_var);
      grps = cache(key_gname);
    else
      [means vars grps] = grpstats(T1(:,2), T1(:,1), {'mean', 'var', 'gname'});
      cache(key_mean) = means;
      cache(key_var) = vars;
      cache(key_gname) = grps;
    end
   
    grps = str2num(char(grps{:,1}));
    mu_v(grps, 2) = means;
    mu_v(:,3) = mu_v(:,2) .^ 2;
    var_v(grps, 2) = vars;

    % estimate := a - b + c

    % calculate 'a' first
    a_common = sum(a_v(:,2) .* mu_v(:,2) .* b_v(:,2))^2;
    a1 = (1/e2 - 1/p) * sum(a_v(:,3) .* mu_v(:,3) .* b_v(:,2)) / a_common;
    a2 = (1/e1 - 1/p) * sum(a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,3)) / a_common; 
    a3 = (p/(e1*e2) - 1/e1 - 1/e2 + 1/p) * sum(a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,2)) / a_common;
    a4 = ((1-p)/p) * sum(a_v(:,3) .* mu_v(:,3) .* b_v(:,3)) / a_common;
    a = a1 + a2 + a3 + a4;

    % calculate 'b'
    b_common = sum(a_v(:,2) .* mu_v(:,2) .* b_v(:,2)) / sum(a_v(:,2) .* b_v(:,2))^3;
    b1 = (p/(e1*e2) - 1/e1 - 1/e2 + 1/p) * sum(a_v(:,2) .* mu_v(:,2) .* b_v(:,2)) * b_common;
    b2 = (1/e1 - 1/p) * sum(a_v(:,2) .* mu_v(:,2) .* b_v(:,3)) * b_common;
    b3 = (1/e2 - 1/p) * sum(a_v(:,3) .* mu_v(:,2) .* b_v(:,2)) * b_common;
    b4 = ((1-p)/p) * sum(a_v(:,3) .* mu_v(:,2) .* b_v(:,3)) * b_common;
    b = b1 + b2 + b3 + b4;

    % calculate 'c'
    c_common = sum(a_v(:,2) .* mu_v(:,2) .* b_v(:,2))^2 / sum(a_v(:,2) .* b_v(:,2))^4;
    c1 = (1/e2 - 1/p) * sum(a_v(:,3) .* b_v(:,2)) * c_common;
    c2 = (1/e1 - 1/p) * sum(a_v(:,2) .* b_v(:,3)) * c_common;
    c3 = (p/(e1*e2) - 1/e1 - 1/e2 + 1/p) * sum(a_v(:,2) .* b_v(:,2)) * c_common;
    c4 = ((1-p)/p) * sum(a_v(:,3) .* b_v(:,3)) * c_common;
    c = c1 + c2 + c3 + c4;

    estimate = a - b + c;
  else
    fprintf("Unsupported agg function: %s", aggFunc);
    return;
  end
end