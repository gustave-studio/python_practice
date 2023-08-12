import sys

input_price = input('insert:')
product_price = input('product:')
change = int(input_price) - int(product_price)

if not input_price.isdecimal():
    print('整数を入力してください')
    sys.exit()

if change < 0:
    print('金額が不足しています')
    sys.exit()

coin_type = [ 5000, 1000, 500, 100, 50, 10, 5, 1 ]

return_coin = {}

for coin in coin_type:
    return_coin[coin] = change // coin
    change = change % coin
    print((f'change: {change}'))

print(return_coin)
