using Zamza.Server.Application;
using Zamza.Server.ConsumerApi;
using Zamza.Server.DataAccess;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDataAccessLayer();
builder.Services.AddApplicationLayer();
builder.Services.AddConsumerApiLayer();

var app = builder.Build();

app.UseHttpsRedirection();

app.AddConsumerApiEndpoints();
app.Services.RunMigrations();

app.Run();
