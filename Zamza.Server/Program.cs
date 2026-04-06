using Zamza.Server.Application;
using Zamza.Server.ConsumerApi;
using Zamza.Server.DataAccess;
using Zamza.Server.UserApi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDataAccessLayer();
builder.Services.AddApplicationLayer();
builder.Services.AddConsumerApiLayer();
builder.Services.AddUserApiLayer();

var app = builder.Build();

app.UseHttpsRedirection();

app.AddConsumerApiEndpoints();
app.AddUserApiEndpoints();
app.Services.RunMigrations();

app.Run();
