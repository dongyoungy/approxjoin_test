function actual = calculate_actual(nRows, nKeys, leftDist, rightDist, aggFunc)

 global cache;
 
 file = ['./actual_results/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '.csv'];
 
 if isfile(file)
%  if false
   actual = readmatrix(file);
 else
   leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
   rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];
  
   % read original tables 
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

    T1tab = tabulate(T1(:,1));
    T2tab = tabulate(T2(:,1));

    T1freq = [1:nKeys]';
    T2freq = [1:nKeys]';

    T1freq(T1tab(:,1), 2) = T1tab(T1tab(:,1), 2);
    T2freq(T2tab(:,1), 2) = T2tab(T2tab(:,1), 2);
  
   if strcmp(aggFunc, 'count')
     actual = sum(T1freq(:,2) .* T2freq(:,2));
   elseif strcmp(aggFunc, 'sum')
     key_mean = [leftFile '_mean'];
     key_gname = [leftFile '_gname'];
     if cache.isKey(key_mean) && cache.isKey(key_gname)
       means = cache(key_mean);
       grps = cache(key_gname);
     else
       [means grps] = grpstats(T1(:,2), T1(:,1), {'mean', 'gname'});
       cache(key_mean) = means;
       cache(key_gname) = grps;
     end
     
     mu = [1:nKeys]';
     grps = str2num(char(grps{:,1}));
     mu(grps, 2) = means;
     
     actual = sum(mu(:,2) .* T1freq(:,2) .* T2freq(:,2));
          
   elseif strcmp(aggFunc, 'avg')  
     key_mean = [leftFile '_mean'];
     key_gname = [leftFile '_gname'];
     if cache.isKey(key_mean) && cache.isKey(key_gname)
       means = cache(key_mean);
       grps = cache(key_gname);
     else
       [means grps] = grpstats(T1(:,2), T1(:,1), {'mean', 'gname'});
       cache(key_mean) = means;
       cache(key_gname) = grps;
     end
     
     mu = [1:nKeys]';
     grps = str2num(char(grps{:,1}));
     mu(grps, 2) = means;
     
     actual = sum(mu(:,2) .* T1freq(:,2) .* T2freq(:,2)) / sum(T1freq(:,2) .* T2freq(:,2));
    else
     fprintf("Unsupported agg function: %s", aggFunc);
     return;
   end
 end
 
 writematrix(actual, file); 
end