import numpy
import os
import torch
import warnings
warnings.filterwarnings('ignore')

save_model_dir_multiply_five = 'five_times_table_torch.pt'
save_model_dir_multiply_ten = 'ten_times_table_torch.pt'

class LinearRegression(torch.nn.Module):
    def __init__(self, input_dim=1, output_dim=1):
        super().__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim)  
    def forward(self, x):
        out = self.linear(x)
        return out

def train(x, y):
    model = LinearRegression()
    optimizer = torch.optim.Adam(model.parameters())
    loss_fn = torch.nn.L1Loss()

    epochs = 10000
    tensor_x = torch.from_numpy(x)
    tensor_y = torch.from_numpy(y)
    for epoch in range(epochs):
        y_pred = model(tensor_x)
        loss = loss_fn(y_pred, tensor_y)
        model.zero_grad()
        loss.backward()
        optimizer.step()

    return model

# train 5 times model
x = numpy.arange(0, 100, dtype=numpy.float32).reshape(-1, 1)
y = (x * 5).reshape(-1, 1)

five_times_model = train(x, y)
torch.save(five_times_model.state_dict(), save_model_dir_multiply_five)
print(os.path.exists(save_model_dir_multiply_five)) # Verify that the model is saved.

# 10 times model
x = numpy.arange(0, 100, dtype=numpy.float32).reshape(-1, 1)
y = (x * 10).reshape(-1, 1)

ten_times_model = train(x, y)
torch.save(ten_times_model.state_dict(), save_model_dir_multiply_ten)
print(os.path.exists(save_model_dir_multiply_ten)) # verify if the model is saved