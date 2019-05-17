function generate_preset_sample_pair(nRows, nKeys, leftDist, rightDist, p, q, num_sample)

  global cache;
  
  sample_dir = '/Volumes/HP_SimpleSave/approxjoin_data/preset_sample_data/';
  % sample_dir = './preset_sample_data/';
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
  
  % create sample pair
  q1 = q;
  q2 = q;

  for sampleIdx = 1:num_sample
    leftSample = [sample_dir num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' num2str(p) '_' num2str(q) '_s1_' num2str(sampleIdx) '.mat'];
    rightSample = [sample_dir num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' num2str(p) '_' num2str(q) '_s2_' num2str(sampleIdx) '.mat'];
    
    % check whether sample file already exists
    if isfile(leftSample) && isfile(rightSample)
      fprintf("samples already exists.\n")
      continue;
    end

    % a pair must use same hash permutation
    keyPermMap = [1:nKeys]';
    keyPermMap(:,2) = randperm(nKeys)';
    
    % attach hash value as the last column
    hash_col_idx = 4;
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
  
  
end