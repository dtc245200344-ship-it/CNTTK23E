using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);

// Bước 3: Đăng ký SignalR
builder.Services.AddSignalR();

var app = builder.Build();

// Dữ liệu mẫu bài tập trong bộ nhớ
var assignments = new List<AssignmentDto>
{
    new(1, "Bai tap REST API", "Thiet ke endpoint...", true),
    new(2, "Bai tap Client", "Viet console client...", true)
};

// Dữ liệu mẫu lượt nộp trong bộ nhớ
var submissions = new List<SubmissionDto>();
var submissionsLock = new object(); // tác vụ nền + HTTP cùng sửa

// GET / : kiểm tra máy chủ đang chạy
app.MapGet("/", () => Results.Ok(new {
    service = "Submission.RealtimeApi",
    status = "running"
}));

// GET /api/assignments : danh sách bài tập
app.MapGet("/api/assignments", () => Results.Ok(assignments));

// GET /api/assignments/{assignmentId:int} : chi tiết một bài tập
app.MapGet("/api/assignments/{assignmentId:int}", (int assignmentId) => {
    var assignment = assignments.FirstOrDefault(item => item.Id == assignmentId);
    return assignment is null
        ? Results.NotFound(new ApiError("ASSIGNMENT_NOT_FOUND", "Khong tim thay bai tap."))
        : Results.Ok(assignment);
});

// POST /api/assignments/{assignmentId:int}/submissions : nộp bài tập
// Bước 4.1: Nhận thêm IHubContext và khởi động chấm bài ở nền
app.MapPost("/api/assignments/{assignmentId:int}/submissions",
(int assignmentId,
 CreateSubmissionRequest request,
 IHubContext<SubmissionHub> hubContext) => {
    var assignment = assignments.FirstOrDefault(i => i.Id == assignmentId);
    if (assignment is null)
        return Results.NotFound(new ApiError("ASSIGNMENT_NOT_FOUND", "Khong tim thay bai tap."));

    if (!assignment.IsOpen)
        return Results.BadRequest(new ApiError("ASSIGNMENT_CLOSED", "Bai tap da dong."));

    if (string.IsNullOrWhiteSpace(request.StudentCode) ||
        string.IsNullOrWhiteSpace(request.RepositoryUrl))
        return Results.BadRequest(new ApiError("INVALID_SUBMISSION", "Thieu ma sinh vien hoac kho ma nguon."));

    var submission = new SubmissionDto(
        Guid.NewGuid(),
        assignmentId,
        request.StudentCode.Trim(),
        request.RepositoryUrl.Trim(),
        "received",
        DateTimeOffset.UtcNow,
        DateTimeOffset.UtcNow);

    lock (submissionsLock)
    {
        submissions.Add(submission);
    }

    // khởi động chấm bài ở nền, KHÔNG chờ
    _ = SimulateGradingAsync(submission.Id, submissions,
        submissionsLock, hubContext);

    return Results.Created($"/api/submissions/{submission.Id}", submission);
});

// GET /api/submissions/{submissionId:guid} : đọc lại lượt nộp theo Id
app.MapGet("/api/submissions/{submissionId:guid}", (Guid submissionId) => {
    lock (submissionsLock) {
        var submission = submissions.FirstOrDefault(i => i.Id == submissionId);
        return submission is null
            ? Results.NotFound(new ApiError("SUBMISSION_NOT_FOUND", "Khong tim thay luot nop."))
            : Results.Ok(submission);
    }
});

// Bước 3: Ánh xạ Hub
app.MapHub<SubmissionHub>("/hubs/submissions");

app.Run();

// Bước 4.2: Cập nhật dữ liệu trước, rồi mới đẩy thông báo tới đúng nhóm
static async Task SimulateGradingAsync(Guid id, List<SubmissionDto> list,
    object gate, IHubContext<SubmissionHub> hub) {
    await Task.Delay(TimeSpan.FromSeconds(1));
    await UpdateStatusAsync(id, "processing", list, gate, hub);
    await Task.Delay(TimeSpan.FromSeconds(2));
    await UpdateStatusAsync(id, "graded", list, gate, hub);
}

static async Task UpdateStatusAsync(Guid id, string status,
    List<SubmissionDto> list, object gate, IHubContext<SubmissionHub> hub) {
    SubmissionDto? updated = null;
    lock (gate) {    // ① cập nhật dữ liệu
        var i = list.FindIndex(s => s.Id == id);
        if (i >= 0) { updated = list[i] with { Status = status,
                                               UpdatedAt = DateTimeOffset.UtcNow };
                      list[i] = updated; }
    }
    if (updated is null) return;
    var msg = new SubmissionStatusMessage(updated.Id, updated.Status,
                                          updated.UpdatedAt);    // ② dữ liệu tối thiểu
    Console.WriteLine($"[Push] {id.ToString()[..8]} -> {status}");
    await hub.Clients.Group(SubmissionGroups.ForSubmission(id))
        .SendAsync("SubmissionStatusChanged", msg);    // ③ đẩy đúng nhóm
}

// Bước 3: Hub mỏng và lớp tiện ích sinh tên nhóm
sealed class SubmissionHub : Hub {
    public Task WatchSubmission(string submissionId) {
        return Groups.AddToGroupAsync(
            Context.ConnectionId,
            SubmissionGroups.ForSubmission(submissionId));
    }
}

static class SubmissionGroups {
    public static string ForSubmission(Guid id) => ForSubmission(id.ToString());
    public static string ForSubmission(string id) => $"submission:{id}";
}

// DTO: các hợp đồng dữ liệu qua mạng
record AssignmentDto(int Id, string Title, string Description, bool IsOpen);
record CreateSubmissionRequest(string StudentCode, string RepositoryUrl);
record SubmissionDto(Guid Id, int AssignmentId, string StudentCode,
    string RepositoryUrl, string Status, DateTimeOffset ReceivedAt,
    DateTimeOffset UpdatedAt);
record SubmissionStatusMessage(Guid SubmissionId, string Status, DateTimeOffset UpdatedAt);
record ApiError(string Code, string Message);
