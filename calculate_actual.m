function actual = calculate_actual(nRows, nKeys, leftDist, rightDist, aggFunc)
 file = ['./actual_results/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_' rightDist '_' aggFunc '.csv'];
 if isfile(file)
   actual = readmatrix(file);
 else
   leftFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' leftDist '_1.csv'];
   rightFile = ['./raw_data/' num2str(nRows) 'n_' num2str(nKeys) 'k_' rightDist '_2.csv'];
  
   % read original tables
   T1 = readmatrix(leftFile);
   T2 = readmatrix(rightFile);
  
   if strcmp(aggFunc, 'count')
     T1tab = tabulate(T1(:,1));
     T2tab = tabulate(T2(:,1));
 
     T1freq = [1:nKeys]';
     T2freq = [1:nKeys]';

     T1freq(T1tab(:,1), 2) = T1tab(T1tab(:,1), 2);
     T2freq(T2tab(:,1), 2) = T2tab(T2tab(:,1), 2);

     actual = sum(T1freq(:,2) .* T2freq(:,2));
   elseif strcmp(aggFunc, 'sum')
   elseif strcmp(aggFunc, 'avg')        
   else
     fprintf("Unsupported agg function: %s", aggFunc);
     return;
   end
 end
 
 writematrix(actual, file); 
end