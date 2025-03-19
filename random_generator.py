import random

def generate_random_sequence(length, start=0, end=100):
    """
    生成一个指定长度的随机数序列。

    参数:
    length (int): 序列的长度。
    start (int): 随机数的最小值（包含）。
    end (int): 随机数的最大值（包含）。

    返回:
    list: 包含随机数的列表。
    """
    return [random.randint(start, end) for _ in range(length)]

# 示例用法
length = 20  # 生成10个随机数
start = 1    # 随机数的最小值
end = 100    # 随机数的最大值

random_sequence = generate_random_sequence(length, start, end)
def print_sequence(sequence):
    for split_point in sequence:
        print(split_point,end="")

# def dfs(random_sequence, start, end, cost, exec_sequence):
#     result = cost
#     if start == len(random_sequence):
#         print_sequence(exec_sequence)
#         return
#     for i in range(start,end):





print("生成的随机数序列:", random_sequence)

