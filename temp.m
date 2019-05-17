res = [];
for i=1:6
p = preset_result{2,1,i,3};r = p.results;
res(:, end+1) = [mean(r(~isnan(r))) var(r(~isnan(r))) p.estimated_var p.vmr]';
end
res

results_var = {};
results_vmr = {};
for d = 1:3
  for a = 1:2
    res = [];
    res_vmr = [];
    for i=1:8
    res(i)=dec_our_result{2,d,i,a}.var;
    res_vmr(i)=dec_our_result{2,d,i,a}.vmr;
    end
    results_var{d,a} = res';
    results_vmr{d,a} = res_vmr';
  end
end

preset_results_var = {};
preset_results_vmr = {};
for d = 1:3
  for a = 1:2
    res = [];
    res_vmr = [];
    for i=1:8
      for p=1:6
        res(i,p)=dec_preset_result{2,d,i,p,a}.var;
        res_vmr(i,p)=dec_preset_result{2,d,i,p,a}.vmr;
      end
    end
    preset_results_var{d,a} = res;
    preset_results_vmr{d,a} = res_vmr;
  end
end

%%%%%%

results = {};
for d = 1:3
  for a = 1:2
    res = [];
    for i=1:8
      for p=1:6
        res(i,p)=dec_preset_result{2,d,i,p,a}.estimated_var;
      end
    end
    results{d,a} = res;
  end
end

results = {};
for d = 1:3
  for a = 1:2
    res = [];
    for i=1:8
      for p=1:6
        res(i,p)=dec_preset_result{2,d,i,p,a}.vmr;
      end
    end
    results{d,a} = res;
  end
end

res = [];
for i=1:8
  for p=1:6
    res(i,p)=dec_preset_result{2,3,i,p,1}.estimated_var;
  end
end
res