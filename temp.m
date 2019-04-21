res = [];
for i=1:6
p = preset_result{2,1,i,3};r = p.results;
res(:, end+1) = [mean(r(~isnan(r))) var(r(~isnan(r))) p.estimated_var p.vmr]';
end
res