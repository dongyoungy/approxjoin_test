class Cond:
    def __init__(self, col_idx, op, value):
        self.col_idx = col_idx  # starts from 0
        self.op = op
        self.value = value

    def apply_df(self, df):
        new_df = None
        if self.op == '=':
            new_df = df[df[self.col_idx] == self.value]
        elif self.op == '<':
            new_df = df[df[self.col_idx] < self.value]
        elif self.op == '<=':
            new_df = df[df[self.col_idx] <= self.value]
        elif self.op == '>=':
            new_df = df[df[self.col_idx] >= self.value]
        elif self.op == '>':
            new_df = df[df[self.col_idx] > self.value]
        else:
            print("Unsupported operation: {}".format(self.op), flush=True)
            return None

        return new_df

    def apply_array(self, arr):
        new_arr = None
        if self.op == '=':
            new_arr = arr[arr[:, self.col_idx] == self.value]
        elif self.op == '<':
            new_arr = arr[arr[:, self.col_idx] < self.value]
        elif self.op == '<=':
            new_arr = arr[arr[:, self.col_idx] <= self.value]
        elif self.op == '>=':
            new_arr = arr[arr[:, self.col_idx] >= self.value]
        elif self.op == '>':
            new_arr = arr[arr[:, self.col_idx] > self.value]
        else:
            print("Unsupported operation: {}".format(self.op), flush=True)
            return None

        return new_arr
