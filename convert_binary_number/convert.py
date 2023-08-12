n = 18


def convert(n, base):
    result = ''

    while n > 0:
        result = str(n % base) + result
        print(f'result: {result}')
        n //= base

    return result

print(convert(n, 2))