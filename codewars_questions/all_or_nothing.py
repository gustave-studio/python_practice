def possibly_perfect(key, answers):
    check_list = []

    for index, answer in enumerate(answers):
        if key[index] == '_':
            continue
        
        check_list.append(answer == key[index])

    return all(result == check_list[0] for result in check_list)