function generate_two_level_sample(nRows, nKeys, leftDist, rightDist, budget, sampleIdx)

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
  
  
  key_q_min = [leftFile '__' rightFile '_' num2str(budget) '_2lv_qmin'];
  key_C = [leftFile '__' rightFile '_' num2str(budget) '_2lv_C'];
  
  if cache.isKey(key_q_min) && cache.isKey(key_C)
    q_min = cache(key_q_min);
    C = cache(key_C);
  else
    fn = @(q)two_level_sample_func(keys,a_v,b_v,q);
    q0 = 0.5;
    
    A = [];
    b = [];
    Aeq = [];
    beq = [];
    lb = 0.0;
    ub = 1.0;
    [q_min, fval] = fmincon(fn, q0, A, b, Aeq, beq, lb, ub);
    
    sig = sigma1(keys, a_v, b_v, q_min);
    
    const1 = @(C) sum(C * sqrt( (sig + a_v(:,3) .* b_v(:,3)) ./ (2 + q_min * (a_v(:,2) + b_v(:,2) - 2)  )) .* (2 + q_min * (a_v(:,2) + b_v(:,2) - 2)  )) - (nRows * budget);
    
    C0 = 1;
    
    % calculate C
    C = fsolve(const1, C0);
    
    cache(key_q_min) = q_min;
    cache(key_C) = C;
  end

  
  sig = sigma1(keys, a_v, b_v, q_min);
  % calculate p_v's
  p = C * sqrt( (sig + a_v(:,3) .* b_v(:,3)) ./ (2 + q_min * (a_v(:,2) + b_v(:,2) - 2)  ));
 
  q = [];
  q(1:nKeys, 1) = q_min;
  
  % a pair must use same hash permutation
  keyPermMap = [1:nKeys]';
  keyPermMap(:,2) = randperm(nKeys)';
  
  hash_col_idx = 4;
  T1(:, hash_col_idx) = keyPermMap(T1(:,1),2);
  T2(:, hash_col_idx) = keyPermMap(T2(:,1),2);
  
  % Sample S1 = A first
  p_map = p(T1(:,1));
  S = T1(find(mod(T1(:,hash_col_idx), 100000) <= (p_map * 100000)), :);

  S1 = [];
  S1_c = zeros(1, nKeys);
  S1_sentry = [];
  for i = 1:size(S,1)
    v = S(i, 1);
    
    if S1_c(v) == 0
      S1_sentry(v,:) = S(i, 1:3);
      S1_c(v) = 1;
    else
      S1_c(v) = S1_c(v) + 1;
      if rand < (1 / S1_c(v))
        S1_sentry(v,:) = S(i, 1:3);
      end
    end
    
    newQ = q(v);
    if p(v) > 1 && q(v) == q_min
      x = a_v(v,2) + b_v(v,2) - 2;
      val = p(v) * (2 + q(v) * x);
      newQ = (val - 2) / x;
      q(v) = newQ;
    end
    
    if rand < newQ
      S1(end+1, :) = S(i, 1:3);
    end
    
  end
  
  % remove sentries
  if ~isempty(S1)
    [ia, ib] = ismember(S1, S1_sentry, 'rows');
    S1(ia, :) = [];
  end
  
   % Sample S2 = B
  p_map = p(T2(:,1));
  S = T2(find(mod(T2(:,hash_col_idx), 100000) <= (p_map * 100000)), :);
  
  S2 = [];
  S2_c = zeros(1, nKeys);
  S2_sentry = [];
  for i = 1:size(S,1)
    v = S(i, 1);
    
    if S2_c(v) == 0
      S2_sentry(v,:) = S(i, 1:3);
      S2_c(v) = 1;
    else
      S2_c(v) = S2_c(v) + 1;
      if rand < (1 / S2_c(v))
        S2_sentry(v,:) = S(i, 1:3);
      end
    end
    
    newQ = q(v);
    if p(v) > 1 && q(v) == q_min
      x = a_v(v,2) + b_v(v,2) - 2;
      val = p(v) * (2 + q(v) * x);
      newQ = (val - 2) / x;
      q(v) = newQ;
    end
    
    if rand < newQ
      S2(end+1, :) = S(i, 1:3);
    end
    
  end
  % remove sentries
  if ~isempty(S2)
    [ia, ib] = ismember(S2, S2_sentry, 'rows');
    S2(ia, :) = [];
  end
  
    % write sample files
  save(leftSample, 'S1', 'p', 'q', 'S1_c', 'budget');
  save(rightSample, 'S2', 'p', 'q', 'S2_c', 'budget');
end