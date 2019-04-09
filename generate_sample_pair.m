function generate_sample_pair(nRows, nKeys, leftDist, rightDist, aggFunc, sampleIdx, isCentralized)

  global cache;

  if nargin < 7
    isCentralized = true;
  end

  if isCentralized
    folder = 'centralized';
  else
    folder = 'decentralized';
  end
  
  leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
  rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];

  leftSample = ['./' folder '/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '_s1_' num2str(sampleIdx) '.mat'];
  rightSample = ['./' folder '/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '_s2_' num2str(sampleIdx) '.mat'];
  
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
  
  % a pair must use same hash permutation
  keyPermMap = [1:nKeys]';
  keyPermMap(:,2) = randperm(nKeys)';
  
  % calculate sampling parameters for given data (p,q1,q2)
  
  % sampling budget
  e1 = 0.01; e2 = 0.01;
  keys = [1:nKeys]';

  % build a table for a_v,b_v,a_v^2,b_v^2
  % a_v --> a_v(:,2)
  % a_v^2 --> a_v(:,3)
  % b_v --> b_v(:,2)
  % b_v^2 --> b_v(:,3)
  
  a = tabulate(T1(:,1));
  a_v = keys;
  a_v(a(:,1), 2) = a(a(:,1), 2);
  a_v(:,3) = a_v(:,2) .^ 2;
  
  b = tabulate(T2(:,1));
  b_v = keys;
  b_v(b(:,1), 2) = b(b(:,1), 2);
  b_v(:,3) = b_v(:,2) .^ 2;

  estimate = 0;

  if isCentralized
    if strcmp(aggFunc, 'count')
      % calculate first sum (one in the numerator)
      sum1 = sum(a_v(:,3) .* b_v(:,3) - a_v(:,3) .* b_v(:,2) - a_v(:,2) .* b_v(:,3) + a_v(:,2) .* b_v(:,2));
      
      % calculate second sum (one in the denominator)
      sum2 = sum(a_v(:,2) .* b_v(:,2));
      
      val = sqrt(e1 * e2 * sum1 / sum2);
      
      p = min([1 max([e1 e2 val])]);
      fprintf("p = %f\n", p);
      
      q1 = e1 / p;
      q2 = e2 / p;

      % calculate theoretical estimate
      v1 = (1/p - 1) * sum(a_v(:,3) .* b_v(:,3));
      v2 = (1/e2 - 1/p) * sum(a_v(:,3) .* b_v(:,2));
      v3 = (1/e1 - 1/p) * sum(a_v(:,2) .* b_v(:,3));
      v4 = (p/(e1*e2) - 1/e1 - 1/e2 + 1/p) * sum(a_v(:,2) .* b_v(:,2));

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
      a_star = max(a_v(:,2));
      
      % calculate first sum in the formula
      sum1 = sum( a_v(:,3) .* mu_v(:,3) .* b_v(:,3) );
      
      % second sum
      sum2 = sum( a_v(:,3) .* mu_v(:,2) .* b_v(:,2) );
      
      % third.. and so on
      sum3 = sum( a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,3) );
      
      sum4 = sum( a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,2) );
      
      sum5 = sum( a_star * b_v(:,2) );
      
      % calculate the value
      val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5;
      val = sqrt(val);
      
      p = min([1 max([e1 e2 val])]);
      fprintf("p = %f\n", p);
      
      q1 = e1 / p;
      q2 = e2 / p;

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
      
      if cache.isKey(key_mean) && cache.isKey(key_gname) && cache.isKey(key_var)
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
      
      % calculate A,B,C,D
      A_denom = sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) )^2;
      A1 = sum( a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,2) ) / A_denom;
      A2 = sum( a_v(:,3) .* mu_v(:,3) .* b_v(:,2) ) / A_denom;
      A3 = sum( a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,3) ) / A_denom;
      A4 = sum( a_v(:,3) .* mu_v(:,3) .* b_v(:,3) ) / A_denom;
      A = A1 - A2 - A3 + A4;
      
      B_denom = sum( a_v(:,2) .* b_v(:,2) )^3;
      B1 = sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) ) / B_denom;
      B2 = sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,3) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) ) / B_denom;
      B3 = sum( a_v(:,3) .* mu_v(:,2) .* b_v(:,2) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) ) / B_denom;
      B4 = sum( a_v(:,3) .* mu_v(:,3) .* b_v(:,2) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) ) / B_denom;
      B = B1 - B2 - B3 + B4;
      
      C_denom = sum( a_v(:,2) .* b_v(:,2) )^4;
      C1 = sum( a_v(:,2) .* b_v(:,2) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) )^2 / C_denom;
      C2 = sum( a_v(:,3) .* b_v(:,2) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) )^2 / C_denom;
      C3 = sum( a_v(:,2) .* b_v(:,3) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) )^2 / C_denom;
      C4 = sum( a_v(:,3) .* b_v(:,3) ) * sum( a_v(:,2) .* mu_v(:,2) .* b_v(:,2) )^2 / C_denom;
      C = C1 - C2 - C3 + C4;
      
      D = (1 / e1 * e2) * (A1 - B1 + C1);
      
      val = sqrt((A - B + C) / D);
      
      p = min([1 max([e1 e2 val])]);
      fprintf("p = %f\n", p);
      
      q1 = e1 / p;
      q2 = e2 / p;
    else
      fprintf("Unsupported agg function: %s", aggFunc);
      return;
    end
  else % if decentralized
    if strcmp(aggFunc, 'count')
      a_star = max(a_v(:,2));
      n_b = nRows;
      sum1 = e1 * e2 * (a_star^2 * n_b^2 + a_star^2 * n_b + a_star * n_b^2 + a_star * n_b);
      sum2 = nKeys * a_star * n_b;
      val = sqrt(sum1 / sum2);

      p = min([1 max([e1 e2 val])]);
      fprintf("p = %f\n", p);

      q1 = e1 / p;
      q2 = e2 / p;

      % calculate theoretical estimate
      v1 = (1/p - 1) * (a_star^2 * n_b^2);
      v2 = (1/e2 - 1/p) * (a_star^2 * n_b);
      v3 = (1/e1 - 1/p) * (a_star .* n_b^2);
      v4 = (p/(e1*e2) - 1/e1 - 1/e2 - 1/p) * (a_star * n_b);

      estimate = v1 + v2 + v3 + v4;
    elseif strcmp(aggFunc, 'sum')
      a_star = max(a_v(:,2));
      n_b = nRows;
      mu_v = [1:nKeys]';
      var_v = [1:nKeys]';
    
      key_mean = [leftFile '_mean'];
      key_var = [leftFile '_var'];
      key_gname = [leftFile '_gname'];
      
      if cache.isKey(key_mean) && cache.isKey(key_gname) && cache.isKey(key_var)
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

      % calculate v1 and v2
      v = [1:nKeys]';
      v(:,2) = a_v(:,3) .* mu_v(:,3);
      v(:,3) = a_v(:,2) .* (mu_v(:,3) + var_v(:,2));
      [m1 v1] = max(v(:,2));
      [m2 v2] = max(v(:,3));

      a_vi = [a_v(v1, 2) a_v(v2, 2)];
      mu_vi = [mu_v(v1, 2) mu_v(v2,2)];
      var_vi = [var_v(v1, 2) var_v(v2,2)]; 

      h = @(p, e1, e2, a, b, mu, sig) (1/e2 - 1/p) * a^2 * mu^2 * b + (1/e1 - 1/p) * a * (mu^2 + sig) * b^2 + (p/(e1*e2) - 1/e1 - 1/e2 + 1/p) * a * (mu^2 + sig) * b + (1/p - 1) * a^2 * mu^2 * b^2;

      % p^2 coeff
      h_p2 = @(e1, e2, a, b, mu, sig) (a * (mu^2 + sig) * b^2) / (e1*e2);
      % p coeff
      h_p = @(e1, e2, a, b, mu, sig) (1/e2) * a^2 * mu^2 * b + (1/e1) * a * (mu^2 + sig) * b^2 - (1/e1 + 1/e2) * a * (mu^2 + sig) * b - a^2 * mu^2 * b^2;
      % const terms
      h_const = @(e1, e2, a, b, mu, sig) a * (mu^2 + sig) * b + a^2 + mu^2 + b^2 - a^2 * mu^2 * b - a * (mu^2 + sig) * b^2;

      % quadratic equation for h1(p) - h2(p) = 0
      eq = [h_p2(e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) - h_p2(e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2)), h_p(e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) - h_p(e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2)), h_const(e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) - h_const(e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))];

      r = roots(eq);
      if isempty(r)
         % calculate first sum in the formula
        sum1 = sum(a_v(v1, 3) .* mu_v(v1, 3) .* n_b^2);

        % second sum
        sum2 = sum(a_v(v1, 3) .* mu_v(v1, 2) .* n_b);

        % third.. and so on
        sum3 = sum(a_v(v1, 2) .* (mu_v(v1, 3) + var_v(v1, 2)) .* n_b^2);

        sum4 = sum(a_v(v1, 2) .* (mu_v(v1, 3) + var_v(v1, 2)) .* n_b);

        sum5 = sum(a_v(v1, 2) * n_b);

        % calculate the value
        val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5;
        val = sqrt(val);

        p = min([1 max([e1 e2 val])])
      else
        p1 = r(1);
        p2 = r(2);
        % calculate first sum in the formula
        sum1 = sum(a_v(v1, 3) .* mu_v(v1, 3) .* n_b^2);

        % second sum
        sum2 = sum(a_v(v1, 3) .* mu_v(v1, 2) .* n_b);

        % third.. and so on
        sum3 = sum(a_v(v1, 2) .* (mu_v(v1, 3) + var_v(v1, 2)) .* n_b^2);

        sum4 = sum(a_v(v1, 2) .* (mu_v(v1, 3) + var_v(v1, 2)) .* n_b);

        sum5 = sum(a_v(v1, 2) * n_b);

        % calculate the value
        val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5;
        val = sqrt(val);

        p3 = min([1 max([e1 e2 val])])

        sum1 = sum(a_v(v2, 3) .* mu_v(v2, 3) .* n_b^2);
        sum2 = sum(a_v(v2, 3) .* mu_v(v2, 2) .* n_b);
        sum3 = sum(a_v(v2, 2) .* (mu_v(v2, 3) + var_v(v2, 2)) .* n_b^2);
        sum4 = sum(a_v(v2, 2) .* (mu_v(v2, 3) + var_v(v2, 2)) .* n_b);
        sum5 = sum(a_v(v2, 2) * n_b);
        val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5;
        val = sqrt(val);

        p4 = min([1 max([e1 e2 val])])

        p5 = max(e1, e2);

        pval = [p1; p2; p3; p4; p5];
        pval(1,2) = max([h(p1, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p1, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
        pval(2,2) = max([h(p2, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p2, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
        pval(3,2) = max([h(p3, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p3, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
        pval(4,2) = max([h(p4, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p4, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
        pval(5,2) = max([h(p5, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p5, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);

        pval(find(~isreal(pval(:,1))), :) = [];
        pval(find(pval(:,1) < max(e1, e2)), :) = [];

        [m i] = min(pval(:,2));

        p = pval(i,1);

      end
        
      fprintf("p = %f\n", p);

      q1 = e1 / p;
      q2 = e2 / p;
    else
      fprintf("Unsupported agg function: %s", aggFunc);
      return;
    end
  end
  
  
  % create sample pair
  
  % attach hash value as the last column
  hash_col_idx = 4;
%   T1(:, hash_col_idx) = get_hash(keyPermMap(T1(:,1),2));
%   T2(:, hash_col_idx) = get_hash(keyPermMap(T2(:,1),2));
  T1(:, hash_col_idx) = keyPermMap(T1(:,1),2);
  T2(:, hash_col_idx) = keyPermMap(T2(:,1),2);
  
  % universe sampling first
  S1 = T1(find(mod(T1(:,hash_col_idx), 100000) <= (p * 100000)), :);
  S2 = T2(find(mod(T2(:,hash_col_idx), 100000) <= (p * 100000)), :);
  
  prob_col_idx = 5;
  % attach rand column for uniform sampling
  S1(:,prob_col_idx) = rand(size(S1,1), 1);
  S2(:,prob_col_idx) = rand(size(S2,1), 1);
  
  % uniform sample
  S1 = S1(find(S1(:,prob_col_idx) <= q1), [1:3]);
  S2 = S2(find(S2(:,prob_col_idx) <= q2), [1:3]);
  
  p1 = p;
  p2 = p;
  
  % write sample files
  save(leftSample, 'S1', 'p1', 'q1');
  save(rightSample, 'S2', 'p2', 'q2');
  
end