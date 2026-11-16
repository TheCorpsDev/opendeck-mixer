using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace OpenDeckMixer;

/// <summary>
/// Collegamento con OpenDeck tramite il protocollo dei plugin Stream Deck
/// (WebSocket JSON). Tutti gli eventi e gli aggiornamenti periodici passano da
/// una coda con un solo consumatore, così le chiamate audio non si accavallano.
/// </summary>
public sealed class Plugin(int port, string pluginUuid, string registerEvent)
{
    public const string MasterAction = "it.thecorpscloud.mixer.master";
    public const string AppAction = "it.thecorpscloud.mixer.app";

    private sealed class Instance(string action, string controller, JsonObject settings)
    {
        public string Action { get; } = action;
        public string Controller { get; } = controller;
        public JsonObject Settings { get; set; } = settings;
        public string? LastTitle { get; set; }
        public bool IsKey => Controller != "Encoder";
    }

    private readonly ClientWebSocket _socket = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly Channel<Func<Task>> _work = Channel.CreateUnbounded<Func<Task>>(new() { SingleReader = true });
    private readonly Dictionary<string, Instance> _instances = new();

    public async Task RunAsync()
    {
        await _socket.ConnectAsync(new Uri($"ws://127.0.0.1:{port}"), CancellationToken.None);
        await SendAsync(new JsonObject { ["event"] = registerEvent, ["uuid"] = pluginUuid });
        Log("connesso a OpenDeck");

        var worker = Task.Run(ProcessWorkAsync);
        _ = Task.Run(RefreshLoopAsync);

        await ReceiveLoopAsync();
        _work.Writer.TryComplete();
        await worker;
        Log("connessione chiusa, esco");
    }

    // ── Ricezione ────────────────────────────────────────────────────────

    private async Task ReceiveLoopAsync()
    {
        var buffer = new byte[16 * 1024];
        using var message = new MemoryStream();
        while (_socket.State == WebSocketState.Open)
        {
            WebSocketReceiveResult result;
            try
            {
                result = await _socket.ReceiveAsync(buffer, CancellationToken.None);
            }
            catch (WebSocketException e)
            {
                Log($"errore WebSocket: {e.Message}");
                return;
            }
            if (result.MessageType == WebSocketMessageType.Close) return;
            message.Write(buffer, 0, result.Count);
            if (!result.EndOfMessage) continue;

            var text = Encoding.UTF8.GetString(message.GetBuffer(), 0, (int)message.Length);
            message.SetLength(0);
            if (JsonNode.Parse(text) is JsonObject json)
                _work.Writer.TryWrite(() => HandleAsync(json));
        }
    }

    private async Task ProcessWorkAsync()
    {
        await foreach (var job in _work.Reader.ReadAllAsync())
        {
            try
            {
                await job();
            }
            catch (Exception e)
            {
                Log($"errore: {e}");
            }
        }
    }

    // I tasti mostrano il volume: lo riallineo ogni secondo, così il valore
    // resta giusto anche se lo cambi dal mixer di Windows o da un'altra manopola.
    private async Task RefreshLoopAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync())
        {
            if (!_work.Writer.TryWrite(RefreshKeysAsync)) return;
        }
    }

    private async Task RefreshKeysAsync()
    {
        foreach (var (context, instance) in _instances.ToList())
            if (instance.IsKey)
                await UpdateTitleAsync(context, instance, Read(instance));
    }

    // ── Eventi ───────────────────────────────────────────────────────────

    private async Task HandleAsync(JsonObject json)
    {
        var evt = (string?)json["event"];
        var context = (string?)json["context"];
        var action = (string?)json["action"];
        var payload = json["payload"] as JsonObject;
        if (evt is null || context is null) return;

        // Le impostazioni arrivano anche dentro gli eventi di manopole e tasti: le riallineo
        // sempre, perché non tutti i software inviano didReceiveSettings dopo una modifica.
        if (evt != "willAppear" && payload?["settings"] is JsonObject current && _instances.TryGetValue(context, out var known))
            known.Settings = (JsonObject)current.DeepClone();

        switch (evt)
        {
            case "willAppear":
            {
                var instance = new Instance(
                    action ?? "",
                    (string?)payload?["controller"] ?? "Keypad",
                    payload?["settings"]?.DeepClone() as JsonObject ?? new JsonObject());
                _instances[context] = instance;
                Log($"{evt} {context}: {Describe(instance)}");
                await UpdateTitleAsync(context, instance, Read(instance));
                break;
            }
            case "willDisappear":
                _instances.Remove(context);
                Log($"{evt} {context}");
                break;
            case "didReceiveSettings":
                if (_instances.TryGetValue(context, out var changed))
                {
                    changed.Settings = payload?["settings"]?.DeepClone() as JsonObject ?? new JsonObject();
                    Log($"{evt} {context}: {Describe(changed)}");
                    await UpdateTitleAsync(context, changed, Read(changed));
                }
                break;
            case "dialRotate":
            {
                if (!_instances.TryGetValue(context, out var instance))
                {
                    Log($"{evt} {context}: istanza sconosciuta, ignorato");
                    break;
                }
                var ticks = (int?)payload?["ticks"] ?? 0;
                if (ticks == 0) break;
                var step = Format.Step(instance.Settings);
                if (UsesMediaKeys(instance))
                {
                    // Il volume cambia in modo asincrono: il titolo lo aggiorna il refresh periodico.
                    MediaKeys.ChangeVolume(ticks * step);
                    Log($"{evt} {context} ticks={ticks}: {Describe(instance)} -> tasti volume");
                    break;
                }
                var delta = ticks * step / 100f;
                VolumeState? state;
                if (instance.Action == MasterAction)
                {
                    state = AudioMixer.ChangeMaster(delta);
                }
                else
                {
                    var result = AudioMixer.ChangeApp(Apps(instance), delta);
                    state = result?.State;
                    ShowAppPopup(instance, result);
                }
                Log($"{evt} {context} ticks={ticks}: {Describe(instance)} -> {Outcome(state)}");
                await UpdateTitleAsync(context, instance, state);
                break;
            }
            case "dialDown":
            case "keyDown":
            {
                if (!_instances.TryGetValue(context, out var instance))
                {
                    Log($"{evt} {context}: istanza sconosciuta, ignorato");
                    break;
                }
                if (UsesMediaKeys(instance))
                {
                    MediaKeys.ToggleMute();
                    Log($"{evt} {context}: {Describe(instance)} -> tasto muto");
                    break;
                }
                VolumeState? state;
                if (instance.Action == MasterAction)
                {
                    state = AudioMixer.ToggleMasterMute();
                }
                else
                {
                    var result = AudioMixer.ToggleAppMute(Apps(instance));
                    state = result?.State;
                    ShowAppPopup(instance, result);
                }
                Log($"{evt} {context}: {Describe(instance)} -> {Outcome(state)}");
                await UpdateTitleAsync(context, instance, state);
                break;
            }
            case "sendToPlugin":
                // Il pannello impostazioni chiede quali app stanno usando l'audio.
                if ((string?)payload?["request"] == "apps")
                {
                    var apps = new JsonArray(AudioMixer.ListApps().Select(a => (JsonNode)a.Name).ToArray());
                    await SendAsync(new JsonObject
                    {
                        ["event"] = "sendToPropertyInspector",
                        ["context"] = context,
                        ["payload"] = new JsonObject { ["apps"] = apps },
                    });
                }
                break;
        }
    }

    private static List<string> Apps(Instance instance) => Format.ParseApps((string?)instance.Settings["apps"]);

    /// <summary>
    /// Popup nostro per le app (quello di Windows mostra solo il master).
    /// Se nessuna app dell'elenco sta usando l'audio, lo dice nel popup.
    /// </summary>
    private static void ShowAppPopup(Instance instance, AppVolume? result)
    {
        if (!Format.Popup(instance.Settings)) return;
        var apps = Apps(instance);
        if (apps.Count == 0) return;
        Osd.Show(Format.NormalizeProcess(result?.Name ?? apps[0]), result?.State, result?.ProcessId ?? 0);
    }

    /// <summary>
    /// Il master usa i tasti multimediali (che mostrano il popup di Windows) a meno
    /// che il popup non sia disattivato nelle impostazioni.
    /// </summary>
    private static bool UsesMediaKeys(Instance instance) =>
        instance.Action == MasterAction && Format.Popup(instance.Settings);

    private static string Describe(Instance instance)
    {
        var kind = instance.Action == MasterAction ? "master" : "app";
        if (instance.Action == MasterAction)
            return $"{kind} ({instance.Controller}, passo {Format.Step(instance.Settings)}%, popup {(Format.Popup(instance.Settings) ? "si" : "no")})";
        var apps = Apps(instance);
        var list = apps.Count == 0 ? "nessuna app impostata" : string.Join(", ", apps);
        return $"{kind} [{list}] ({instance.Controller}, passo {Format.Step(instance.Settings)}%)";
    }

    private static string Outcome(VolumeState? state) =>
        state is null ? "nessuna sessione audio trovata" : Format.Title(state.Value);

    private static VolumeState? Read(Instance instance) =>
        instance.Action == MasterAction ? AudioMixer.GetMaster() : AudioMixer.GetApp(Apps(instance))?.State;

    /// <summary>
    /// Mostra il volume come titolo, solo sui tasti: le manopole dell'AKP03E
    /// non hanno display. Invia solo se il testo è cambiato.
    /// </summary>
    private async Task UpdateTitleAsync(string context, Instance instance, VolumeState? state)
    {
        if (!instance.IsKey) return;
        var title = state is null ? "—" : Format.Title(state.Value);
        if (title == instance.LastTitle) return;
        instance.LastTitle = title;
        await SendAsync(new JsonObject
        {
            ["event"] = "setTitle",
            ["context"] = context,
            ["payload"] = new JsonObject { ["title"] = title, ["target"] = 0 },
        });
    }

    // ── Invio ────────────────────────────────────────────────────────────

    private async Task SendAsync(JsonObject message)
    {
        var bytes = Encoding.UTF8.GetBytes(message.ToJsonString());
        await _sendLock.WaitAsync();
        try
        {
            if (_socket.State == WebSocketState.Open)
                await _socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private static void Log(string message) => Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {message}");
}
