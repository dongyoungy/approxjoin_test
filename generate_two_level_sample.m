function generate_two_level_sample(nRows, nKeys, leftDist, rightDist, sampleIdx)

  global cache;
  
  leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
  rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];
  
  leftSample = ['./two_level_sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_s1_' num2str(sampleIdx) '.mat'];
  rightSample = ['./two_level_sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_s2_' num2str(sampleIdx) '.mat'];
  
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
  

  fn = @(q)two_level_sample_func(keys,a_v,b_v,q);
  q0 = 0.5;
  
  A = [];
  b = [];
  Aeq = [];
  beq = [];
  lb = 0.0;
  ub = 1.0;
  [q_min, fval] = fmincon(fn, q0, A, b, Aeq, beq, lb, ub)
  
  sig = sigma1(keys, a_v, b_v, q_min);
  
  const1 = @(C) sum(C * sqrt( (sig + a_v(:,3) .* b_v(:,3)) ./ (2 + q_min * (a_v(:,2) + b_v(:,2) - 2)  )) .* (2 + q_min * (a_v(:,2) + b_v(:,2) - 2)  )) - (nRows * 0.01);
  
  C0 = 1;
  
  
  
  % calculate C
  C = fsolve(const1, C0);
  
  % calculate p_v's
  p = C * sqrt( (sig + a_v(:,3) .* b_v(:,3)) ./ (2 + q_min * (a_v(:,2) + b_v(:,2) - 2)  ));
 
  q = q_min;
  
  % a pair must use same hash permutation
  keyPermMap = [1:nKeys]';
  keyPermMap(:,2) = randperm(nKeys)';
  
  hash_col_idx = 4;
  T1(:, hash_col_idx) = keyPermMap(T1(:,1),2);
  T2(:, hash_col_idx) = keyPermMap(T2(:,1),2);
  
  % Sample S1 = A first
  S1 = [];
  S1_c = zeros(1, nRows);
  S1_sentry = [];
  for i = 1:nRows
    v = T1(i, 1);
    if mod(T1(i, hash_col_idx), 100000) < p(v) * 100000
      if S1_c(v) == 0
        S1_sentry(v,:) = T1(i, 1:3);
        S1_c(v) = 1;     
      else
        S1_c(v) = S1_c(v) + 1;
        if rand < (1 / S1_c(v))
          S1_sentry(v,:) = T1(i, 1:3);
        end
      end
      
      if rand < q
        S1(end+1, :) = T1(i, 1:3);
      end      
    end
  end
  
  % remove sentries
  [ia, ib] = ismember(S1, S1_sentry, 'rows');
  S1(ia, :) = [];
  
   % Sample S2 = B
  S2 = [];
  S2_c = zeros(1, nRows);
  S2_sentry = [];
  for i = 1:nRows
    v = T2(i, 1);
    if mod(T2(i, hash_col_idx), 100000) < p(v) * 100000
      if S2_c(v) == 0
        S2_sentry(v,:) = T2(i, 1:3);
        S2_c(v) = 1;     
      else
        S2_c(v) = S2_c(v) + 1;
        if rand < (1 / S2_c(v))
          S2_sentry(v,:) = T2(i, 1:3);
        end
      end
      
      if rand < q
        S2(end+1, :) = T2(i, 1:3);
      end      
    end
  end
  % remove sentries
  [ia, ib] = ismember(S2, S2_sentry, 'rows');
  S2(ia, :) = [];

    % write sample files
  save(leftSample, 'S1', 'p', 'q', 'S1_c');
  save(rightSample, 'S2', 'p', 'q', 'S2_c');
end