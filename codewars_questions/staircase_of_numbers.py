# 数字の階段を作る
# 1
# 1*2
# 1**3
# 1***4
# 1****5
def pattern(n):
    answer = '1'

    for x in range(1, n + 1):
        if x > 1:
            answer += '\n1' + ('*' * (x -1)) + "{}".format(x)
    
    return answer