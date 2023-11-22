def arr2bin(arr):
    for item in arr:
        if type(item) is not int:
            return False
    
    number = sum(arr)
    
    if number is 0:
        return '0'
    
    result = ''
    
    while number > 0:
        result = str(number % 2) + result
        print(f'result: {result}')
        number //= 2

    return result