function [actual, estimate, p1, q1, p2, q2] = calculate_agg(nRows, nKeys, leftDist, rightDist, aggFunc, sampleIdx)
  leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
  rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];
  
  leftSample = ['./sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '_s1_' num2str(sampleIdx) '.mat'];
  rightSample = ['./sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '_s2_' num2str(sampleIdx) '.mat'];
  
  fprintf("For {%d, %d, %s, %s, %s}:\n", nRows, nKeys, leftDist, rightDist, aggFunc);
  % calculate actual value
  actual = calculate_actual(nRows, nKeys, leftDist, rightDist, aggFunc);
  estimate = 0;
  
  S1Struct = load(leftSample);
  S2Struct = load(rightSample);
  
  S1 = S1Struct.S1;
  p1 = S1Struct.p1;
  q1 = S1Struct.q1;
  
  S2 = S2Struct.S2;
  p2 = S2Struct.p2;
  q2 = S2Struct.q2;
  
  if strcmp(aggFunc, 'count')
    fprintf("actual = %d\n", actual);
    
    S1tab = tabulate(S1(:,1));
    S2tab = tabulate(S2(:,1));
    
    S1freq = [1:nKeys]';
    S2freq = [1:nKeys]';
    
    S1freq(S1tab(:,1), 2) = S1tab(S1tab(:,1), 2);
    S2freq(S2tab(:,1), 2) = S2tab(S2tab(:,1), 2);
    
    estimate = sum(S1freq(:,2) .* S2freq(:,2));
    
    p = min(p1, p2);
    estimate = estimate * (1 / (p * q1 * q2));
    fprintf("estimate = %.3f\n", estimate);
    
    abs_diff = abs(estimate - actual);
    rel_diff = abs_diff / actual;
    
    fprintf("absolute diff. = %.3f\n", abs_diff);
    fprintf("relative diff. = %.3f %%\n", rel_diff * 100);    
  elseif strcmp(aggFunc, 'sum')
   
  elseif strcmp(aggFunc, 'avg')    
    
  else
    fprintf("Unsupported agg function: %s", aggFunc);
    return;
  end
  fprintf("\n");
end
