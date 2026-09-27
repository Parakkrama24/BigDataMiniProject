# Smoke test (Windows PowerShell version): brings up the stack, starts a
# consumer, produces while it's listening, and fails loudly if no messages
# arrive.
#
# Why this exists alongside scripts/smoke.sh: on this project's Windows dev
# machine, invoking `python` from a non-interactive Git Bash session
# (`bash scripts/smoke.sh`) fails to resolve `python` on PATH, because that
# shell doesn't source the profile that translates Windows PATH into a form
# Git Bash can search. PowerShell has no such issue. Teammates on Mac/Linux,
# or with a properly configured Git Bash, should use smoke.sh instead.
#
# Run from the project root:
#   .\scripts\smoke.ps1

$ErrorActionPreference = "Stop"

Write-Host "== Bringing up Docker Compose stack =="
docker compose up -d

Write-Host "== Waiting for Kafka broker to be ready =="
Start-Sleep -Seconds 10

Write-Host "== Ensuring topics exist =="
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
    --topic vitals-stream --bootstrap-server localhost:9092 `
    --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
    --topic vitals-dlq --bootstrap-server localhost:9092 `
    --partitions 1 --replication-factor 1

# Start the consumer FIRST, in the background, so it's already subscribed
# before the producer sends anything. A consumer started AFTER production
# stops (without --from-beginning) will never see those messages -- there
# has to be an overlap where both are running at once.
Write-Host "== Starting consumer (background) =="
$consumerJob = Start-Job -ScriptBlock {
    docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
        --topic vitals-stream --bootstrap-server localhost:9092 `
        --max-messages 5 --timeout-ms 20000
}
Start-Sleep -Seconds 2  # give it time to actually subscribe before we produce

Write-Host "== Producing vitals for 8 seconds =="
$producerProcess = Start-Process -FilePath python -ArgumentList "-m", "ingestion.producer" -PassThru -NoNewWindow
Start-Sleep -Seconds 8
Stop-Process -Id $producerProcess.Id -Force -ErrorAction SilentlyContinue

Write-Host "== Waiting for consumer result =="
# kafka-console-consumer.sh writes its "Processed a total of N messages"
# summary to stderr (normal for this tool, not a real error). Receive-Job
# would otherwise re-throw those lines as terminating NativeCommandErrors
# under $ErrorActionPreference = "Stop", so we explicitly capture errors
# into a variable instead of letting them throw, and treat both streams as
# plain text.
$consumerOutput = Receive-Job -Job $consumerJob -Wait -ErrorAction SilentlyContinue -ErrorVariable consumerErrors
Remove-Job -Job $consumerJob -Force

$allLines = @($consumerOutput) + @($consumerErrors | ForEach-Object { $_.ToString() })
$allLines | ForEach-Object { Write-Host $_ }
$combined = $allLines -join "`n"

if ($combined -notmatch "Processed a total of (\d+) messages") {
    Write-Host "== SMOKE TEST FAILED: could not confirm the consumer ran =="
    exit 1
}
if ($Matches[1] -eq "0") {
    Write-Host "== SMOKE TEST FAILED: consumer received 0 messages =="
    exit 1
}

Write-Host "== Smoke test passed ($($Matches[1]) messages received) =="
