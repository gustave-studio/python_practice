import itertools

# 同じ数字が隣合ってない場合は両サイドに同じ数字を入れる
def numbers_need_friends_too(n):
    l = [int(x) for x in list(str(n))]
    
    answer_list = []
    print(l)
    
    for k, g in itertools.groupby(l):
        num = k
        group = list(g)
        
        one = len(group) == 1
        if one:
            for _ in range(3):
                answer_list.append(num)
        else:
            answer_list.extend(group)
            
    return int(''.join(map(str, answer_list)))