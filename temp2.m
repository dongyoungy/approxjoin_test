diff = [];
for i=100:1:5000
  diff(end+1) = abs( (var(res1(1:i)) - var(res2(1:i))) / var(res1(1:i)) );
end