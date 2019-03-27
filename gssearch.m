function m = gssearch(fn, a, b, v, a_v, b_v)
  tol = 0.00001;
  gr = (sqrt(5) + 1) / 2;

  c = b - (b - a) / gr;
  d = a + (b - a) / gr;
  while abs(c - d) > tol
    val1 = fn(v, a_v, b_v, c);
    val2 = fn(v, a_v, b_v, d);
    if val1 < val2
        b = d;
    else
        a = c;
    end

    % we recompute both c and d here to avoid loss of precision which may lead to incorrect results or infinite loop
    c = b - (b - a) / gr;
    d = a + (b - a) / gr;
  end
  m = (b + a) / 2;
end