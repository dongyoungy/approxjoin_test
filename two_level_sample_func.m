function val = two_level_sample_func(keys, a_v, b_v, q) 
%   fn = @(k) sigma1(k, a_v, b_v, q);
  sig = sigma1(keys, a_v, b_v, q);

  val = sum( sqrt ( (sig + a_v(keys,3) .* b_v(keys,3)) .* (2 + q * (a_v(keys,2) + b_v(keys,2) - 2))  ) )^2;
end