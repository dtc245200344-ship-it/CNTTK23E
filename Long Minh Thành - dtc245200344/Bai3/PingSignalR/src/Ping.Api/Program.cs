using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);

// Bước 1: Đăng ký SignalR
builder.Services.AddSignalR();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(
    new { service = "PingApi", status = "running" }));

// Bước 2: Ánh xạ Hub
app.MapHub<PingHub>("/hubs/ping");

app.Run();

// Bước 3: Phương thức Echo & Bước 5: Phát rộng và nhóm & Bước 6: Vòng đời kết nối
sealed class PingHub : Hub
{
    // Bước 6: Vòng đời kết nối
    public override async Task OnConnectedAsync()
    {
        Console.WriteLine($"[Hub] + {Context.ConnectionId[..8]} ket noi");
        await Clients.All.SendAsync("SystemMessage",
            $"Client {Context.ConnectionId[..8]} da ket noi.");
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex)
    {
        Console.WriteLine($"[Hub] - {Context.ConnectionId[..8]} ngat");
        await Clients.All.SendAsync("SystemMessage",
            $"Client {Context.ConnectionId[..8]} da ngat ket noi.");
        await base.OnDisconnectedAsync(ex);
    }

    // Máy khách gọi Echo -> trả lời riêng người gọi
    public async Task Echo(string message)
    {
        await Clients.Caller.SendAsync(
            "EchoResponse", $"Echo: {message}");
    }

    // Bước 5: Phát rộng và nhóm
    public async Task Broadcast(string message)
        => await Clients.All.SendAsync("BroadcastMessage", message);

    public async Task JoinGroup(string groupName)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, groupName);
        await Clients.Group(groupName).SendAsync("GroupMessage",
            $"{Context.ConnectionId[..8]} da tham gia nhom {groupName}");
    }

    public async Task SendToGroup(string groupName, string message)
        => await Clients.Group(groupName).SendAsync("GroupMessage", message);
}
