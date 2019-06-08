class Cond:
    def __init__(self, col_idx, op, value):
        self.col_idx = col_idx  # starts from 0
        self.op = op
        self.value = value

    def __eq__(self, other):
        if isinstance(other, Cond):
            return self.col_idx == other.col_idx and self.op == other.op and self.value == other.value
        return False

    def __hash__(self):
        return hash(self.col_idx) + hash(self.op) + hash(self.value)

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
