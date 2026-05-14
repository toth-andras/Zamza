FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /zamza

COPY Zamza.sln .
COPY Directory.Build.props .
COPY Directory.Packages.props .
COPY Zamza.Server ./Zamza.Server
COPY Zamza.Server.Application ./Zamza.Server.Application
COPY Zamza.Server.ConsumerApi ./Zamza.Server.ConsumerApi
COPY Zamza.Server.DataAccess ./Zamza.Server.DataAccess
COPY Zamza.Server.Models ./Zamza.Server.Models
COPY Zamza.Server.UserApi ./Zamza.Server.UserApi

WORKDIR /zamza/Zamza.Server
RUN dotnet publish Zamza.Server.csproj -c Release -o /zamza/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /zamza

COPY --from=build /zamza/publish .

EXPOSE 5000
EXPOSE 5249
EXPOSE 9090

ENV ASPNETCORE_URLS=http://+:5000;http://+:5249;http://+:9090

ENTRYPOINT ["dotnet", "Zamza.Server.dll"]