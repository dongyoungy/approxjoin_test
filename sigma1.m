function sigma = sigma1(v, a_v, b_v, q)
  sigma = ((1/q^2) - 1) * (a_v(v,2) - 1) .* (b_v(v,2) - 1) + (1/q - 1) * (b_v(v,2)-1) .* (a_v(v,3) - a_v(v,2) + 1) + (1/q - 1) .* (a_v(v,2) - 1) .* (b_v(v,3) - b_v(v,2) + 1);
  sigma(find(sigma<0)) = 0;
end