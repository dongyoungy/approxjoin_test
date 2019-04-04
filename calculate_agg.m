function [actual, estimate, p1, q1, p2, q2] = calculate_agg(nRows, nKeys, leftDist, rightDist, aggFunc, sampleIdx, isCentralized, useLeft)

  if nargin < 7
    isCentralized = true;
  end

  if nargin < 8
    useLeft = true;
  end

  if isCentralized
    folder = 'centralized';
  else
    folder = 'decentralized';
  end

  leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
  rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];

  leftSampleDist = leftDist;
  rightSampleDist = rightDist;

  if ~isCentralized
    if ~useLeft
      leftSampleDist = rightDist;
      rightSampleDist = leftDist;
    end
  end
  
  leftSample = ['./' folder '/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftSampleDist '_' rightSampleDist '_' aggFunc '_s1_' num2str(sampleIdx) '.mat'];
  rightSample = ['./' folder '/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftSampleDist '_' rightSampleDist '_' aggFunc '_s2_' num2str(sampleIdx) '.mat'];
  
  fprintf("For {%d, %d, %s, %s, %s, %s, %s, %s}:\n", nRows, nKeys, leftDist, rightDist, leftSampleDist, rightSampleDist, aggFunc, folder);
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
    
    rel_diff = abs((estimate - actual) / actual);
%     rel_diff = abs_diff / actual;
    
%     fprintf("absolute diff. = %.3f\n", abs_diff);
    fprintf("percent error = %.3f %%\n", rel_diff * 100);  
  elseif strcmp(aggFunc, 'sum')
    S1tab = tabulate(S1(:,1));
    S2tab = tabulate(S2(:,1));
    
    S1freq = [1:nKeys]';
    S2freq = [1:nKeys]';
    
    S1freq(S1tab(:,1), 2) = S1tab(S1tab(:,1), 2);
    S2freq(S2tab(:,1), 2) = S2tab(S2tab(:,1), 2);
    
    [means grps] = grpstats(S1(:,2), S1(:,1), {'mean', 'gname'});
    mu = [1:nKeys]';
    grps = str2num(char(grps{:,1}));
    mu(grps, 2) = means;
     
    estimate = sum(mu(:,2) .* S1freq(:,2) .* S2freq(:,2));
    
    p = min(p1, p2);
    estimate = estimate * (1 / (p * q1 * q2));
    fprintf("actual = %.3f\n", actual);
    fprintf("estimate = %.3f\n", estimate);
    
    rel_diff = abs((estimate - actual) / actual);
    fprintf("percent error = %.3f %%\n", rel_diff * 100);    
  elseif strcmp(aggFunc, 'avg')    
    S1tab = tabulate(S1(:,1));
    S2tab = tabulate(S2(:,1));
    
    S1freq = [1:nKeys]';
    S2freq = [1:nKeys]';
    
    S1freq(S1tab(:,1), 2) = S1tab(S1tab(:,1), 2);
    S2freq(S2tab(:,1), 2) = S2tab(S2tab(:,1), 2);
    
    [means grps] = grpstats(S1(:,2), S1(:,1), {'mean', 'gname'});
    mu = [1:nKeys]';
    grps = str2num(char(grps{:,1}));
    mu(grps, 2) = means;
     
    estimate_count = sum(S1freq(:,2) .* S2freq(:,2));
    estimate_sum = sum(mu(:,2) .* S1freq(:,2) .* S2freq(:,2));
    
    p = min(p1, p2);
    estimate_sum = estimate_sum * (1 / (p * q1 * q2));
    estimate_count = estimate_count * (1 / (p * q1 * q2));
    estimate = estimate_sum / estimate_count;
    
    fprintf("actual = %.3f\n", actual);
    fprintf("estimate = %.3f\n", estimate);
    
    rel_diff = abs((estimate - actual) / actual);
    fprintf("percent error = %.3f %%\n", rel_diff * 100);       
  else
    fprintf("Unsupported agg function: %s", aggFunc);
    return;
  end
  fprintf("\n");
end
