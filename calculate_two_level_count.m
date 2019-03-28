function [actual, estimate, p1, q1, p2, q2] = calculate_two_level_count(nRows, nKeys, leftDist, rightDist, sampleIdx)
  leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
  rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];
  
  leftSample = ['./two_level_sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_s1_' num2str(sampleIdx) '.mat'];
  rightSample = ['./two_level_sample_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_s2_' num2str(sampleIdx) '.mat'];
  
  fprintf("For {%d, %d, %s, %s}:\n", nRows, nKeys, leftDist, rightDist);
  % calculate actual value
  actual = calculate_actual(nRows, nKeys, leftDist, rightDist, 'count');
  estimate = 0;
  
  S1Struct = load(leftSample);
  S2Struct = load(rightSample);
  
  S1 = S1Struct.S1;
  p = S1Struct.p;
  q = S1Struct.q;
  S1_c = S1Struct.S1_c;
  
  S2 = S2Struct.S2;
  p2 = S2Struct.p;
  q2 = S2Struct.q;
  S2_c = S2Struct.S2_c;
  
  if isempty(S1) || isempty(S2)
    return;
  end
  
  fprintf("actual = %d\n", actual);
  
  S1tab = tabulate(S1(:,1));
  S2tab = tabulate(S2(:,1));
  
  S1freq = [1:nKeys]';
  S2freq = [1:nKeys]';
  
  S1freq(S1tab(:,1), 2) = S1tab(S1tab(:,1), 2);
  S2freq(S2tab(:,1), 2) = S2tab(S2tab(:,1), 2);
  
  isSampled = (S1_c .* S2_c)';
  isSampled(find(isSampled > 1)) = 1;
  p(p>1) = 1;
  v1 = 1./p;
  v2 = ((S1freq(:,2) ./ q) + 1);
  v3 = ((S2freq(:,2) ./ q) + 1);
  v1(isinf(v1)) = 0;
  estimate = sum( isSampled .* v1 .* v2 .* v3);
%   estimate = sum( isSampled .* (1./p) .* ( (S1freq(:,2) / q) + 1) .* ( (S2freq(:,2) / q) + 1) );
  
  fprintf("estimate = %.3f\n", estimate);
  
  rel_diff = abs((estimate - actual) / actual);
  
  fprintf("percent error = %.3f %%\n", rel_diff * 100);
  fprintf("\n");
end
