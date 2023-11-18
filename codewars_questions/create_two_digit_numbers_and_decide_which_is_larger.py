# 2桁の数字を作成し、大きい方を判定
def win_round(you, opp):
    list_of_you = sorted(you, reverse=True)
    list_of_opp = sorted(opp, reverse=True)
    
    number_of_you = str(list_of_you[0]) + str(list_of_you[1])
    number_of_opp = str(list_of_opp[0]) + str(list_of_opp[1])
    
    return number_of_you > number_of_opp