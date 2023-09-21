def barcode_weights(value: str):
            if value[0].isalpha() is True or value[1].isalpha() is True:
                new_value = 'NaN'
                return new_value
            else:
                return value





s = 'XCD69KUI0K'


# print(s[:2])

print(barcode_weights(s))


# ls = [1, 2]

# def cycle(ls):
#     i = 0
#     while True:
#         if i == len(ls) - 1:
#             yield ls[i]
#             i = 0
#         else:
#             yield ls[i]
#             i +=1

# for element in cycle(ls):
#     print(element)


# import random

# def rand():
#   while True:
#     yield random.random()

# rands = rand()
# for num in rand():
#   print(num)