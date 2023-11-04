# nがdivisorで割れる回数を求める。
def divisions(n, divisor):
    num = n
    cnt = 0

    while num >= divisor:
        num //= divisor
        cnt += 1
    return cnt