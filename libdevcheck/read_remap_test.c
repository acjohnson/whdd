#define _FILE_OFFSET_BITS 64
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <sys/wait.h>
#include <inttypes.h>
#include "procedure.h"
#include "ata.h"
#include "scsi.h"

struct read_remap_priv {
    const char *api_str;
    int64_t start_lba;
    enum Api api;
    int64_t end_lba;
    int64_t lba_to_process;
    int fd;
    void *buf;
    AtaCommand ata_command;
    ScsiCommand scsi_command;
    int old_readahead;
    uint64_t current_lba;
    const char *remap_enable_str;
    int64_t remap_timeout_ms;
    int remap_enable;
    uint64_t remapped_count;
};
typedef struct read_remap_priv ReadRemapPriv;

#define SECTORS_AT_ONCE 256
#define BLK_SIZE (SECTORS_AT_ONCE * 512) // FIXME hardcode

// Function to remap sector using hdparm
static int hdparm_remap_sector(uint64_t lba, const char* device_path) {
    char command[512];
    int exit_code;
    pid_t pid;
    int status;

    // Input validation
    if (!device_path || strlen(device_path) == 0) {
        dc_log(DC_LOG_ERROR, "Invalid device path for sector remap\n");
        return 1;
    }

    // Use bundled hdparm if available, otherwise system hdparm
    // Redirect output to /dev/null to prevent disrupting ncurses display
    if (access("./external/hdparm-9.65/hdparm", X_OK) == 0) {
        snprintf(command, sizeof(command), "./external/hdparm-9.65/hdparm --yes-i-know-what-i-am-doing --repair-sector %"PRIu64" %s >/dev/null 2>&1", lba, device_path);
    } else {
        snprintf(command, sizeof(command), "hdparm --yes-i-know-what-i-am-doing --repair-sector %"PRIu64" %s >/dev/null 2>&1", lba, device_path);
    }

    dc_log(DC_LOG_DEBUG, "Attempting to remap sector %"PRIu64" using: %s\n", lba, command);

    pid = fork();
    if (pid == 0) {
        // Child process
        execl("/bin/sh", "sh", "-c", command, NULL);
        exit(127);
    } else if (pid > 0) {
        // Parent process
        if (waitpid(pid, &status, 0) == -1) {
            dc_log(DC_LOG_ERROR, "waitpid failed for remap command\n");
            return 1;
        }

        exit_code = WEXITSTATUS(status);
        if (exit_code == 0) {
            dc_log(DC_LOG_DEBUG, "Successfully remapped sector %"PRIu64"\n", lba);
            return 0;
        } else {
            dc_log(DC_LOG_ERROR, "Failed to remap sector %"PRIu64" (exit code: %d)\n", lba, exit_code);
            return 1;
        }
    } else {
        dc_log(DC_LOG_ERROR, "fork failed for remap command\n");
        return 1;
    }
}

static int SuggestDefaultValue(DC_Dev *dev, DC_OptionSetting *setting) {
    (void)dev;
    if (!strcmp(setting->name, "api")) {
        if (dev->ata_capable)
            setting->value = strdup("ata");
        else
            setting->value = strdup("posix");
    } else if (!strcmp(setting->name, "start_lba")) {
        setting->value = strdup("0");
    } else if (!strcmp(setting->name, "remap_enable")) {
        setting->value = strdup("yes");
    } else if (!strcmp(setting->name, "remap_timeout_ms")) {
        setting->value = strdup("5000");
    } else {
        return 1;
    }
    return 0;
}

static int Open(DC_ProcedureCtx *ctx) {
    int r;
    int open_flags;
    ReadRemapPriv *priv = ctx->priv;

    // Setting context
    if (!strcmp(priv->api_str, "ata"))
        priv->api = Api_eAta;
    else if (!strcmp(priv->api_str, "posix"))
        priv->api = Api_ePosix;
    else
        return 1;
    if (priv->api == Api_eAta && !ctx->dev->ata_capable)
        return 1;

    // Parse remap enable option
    if (!strcmp(priv->remap_enable_str, "yes") || !strcmp(priv->remap_enable_str, "y") || !strcmp(priv->remap_enable_str, "1")) {
        priv->remap_enable = 1;
    } else {
        priv->remap_enable = 0;
    }

    ctx->blk_size = BLK_SIZE;
    priv->current_lba = priv->start_lba;
    priv->end_lba = ctx->dev->capacity / 512;
    priv->lba_to_process = priv->end_lba - priv->start_lba;
    if (priv->lba_to_process <= 0)
        return 1;
    ctx->progress.den = priv->lba_to_process / SECTORS_AT_ONCE;
    if (priv->lba_to_process % SECTORS_AT_ONCE)
        ctx->progress.den++;

    // Initialize remap statistics
    priv->remapped_count = 0;

    if (priv->api == Api_eAta) {
        open_flags = O_RDWR;
    } else {
        r = posix_memalign(&priv->buf, sysconf(_SC_PAGESIZE), ctx->blk_size);
        if (r)
            return 1;

        open_flags = O_RDONLY | O_DIRECT | O_LARGEFILE | O_NOATIME;
    }

    priv->fd = open(ctx->dev->dev_path, open_flags);
    if (priv->fd == -1) {
        dc_log(DC_LOG_FATAL, "open %s fail\n", ctx->dev->dev_path);
        return 1;
    }

    lseek(priv->fd, 512 * priv->start_lba, SEEK_SET);
    r = ioctl(priv->fd, BLKFLSBUF, NULL);
    if (r == -1)
      dc_log(DC_LOG_WARNING, "Flushing block device buffers failed\n");
    r = ioctl(priv->fd, BLKRAGET, &priv->old_readahead);
    if (r == -1)
      dc_log(DC_LOG_WARNING, "Getting block device readahead setting failed\n");
    r = ioctl(priv->fd, BLKRASET, 0);
    if (r == -1)
      dc_log(DC_LOG_WARNING, "Disabling block device readahead setting failed\n");

    return 0;
}

static int Perform(DC_ProcedureCtx *ctx) {
    ssize_t read_ret;
    int ioctl_ret;
    int ret = 0;
    ReadRemapPriv *priv = ctx->priv;
    size_t sectors_to_read = (priv->lba_to_process < SECTORS_AT_ONCE) ? priv->lba_to_process : SECTORS_AT_ONCE;

    // Updating context
    ctx->report.lba = priv->current_lba;
    ctx->report.sectors_processed = sectors_to_read;
    ctx->report.blk_status = DC_BlockStatus_eOk;

    // Preparing to act
    if (priv->api == Api_eAta) {
        memset(&priv->ata_command, 0, sizeof(priv->ata_command));
        memset(&priv->scsi_command, 0, sizeof(priv->scsi_command));
        prepare_ata_command(&priv->ata_command, WIN_VERIFY_EXT /* 42h */, priv->current_lba, sectors_to_read);
        prepare_scsi_command_from_ata(&priv->scsi_command, &priv->ata_command);
    }

    // Timing
    _dc_proc_time_pre(ctx);

    // Acting
    if (priv->api == Api_eAta)
        ioctl_ret = ioctl(priv->fd, SG_IO, &priv->scsi_command);
    else
        read_ret = read(priv->fd, priv->buf, sectors_to_read * 512);

    // Timing
    _dc_proc_time_post(ctx);

    // Error handling
    if (priv->api == Api_eAta) {
        // Updating context
        if (ioctl_ret) {
            ctx->report.blk_status = DC_BlockStatus_eError;
            ret = 1;
        }
        ctx->report.blk_status = scsi_ata_check_return_status(&priv->scsi_command);
    } else {
        if ((int)read_ret != (int)sectors_to_read * 512) {
            // Position of fd is undefined. Set fd position to read next block
            lseek(priv->fd, 512 * priv->current_lba, SEEK_SET);

            // Updating context
            ctx->report.blk_status = DC_BlockStatus_eError;
        }
    }

    // Updating context
    ctx->progress.num++;
    priv->lba_to_process -= sectors_to_read;
    priv->current_lba += sectors_to_read;

    // Check if remapping is enabled and sector is slow
    if (priv->remap_enable && ctx->report.blk_status == DC_BlockStatus_eOk) {
        uint64_t timeout_threshold_us = priv->remap_timeout_ms * 1000; // Convert ms to microseconds
        if (ctx->report.blk_access_time > timeout_threshold_us) {
            dc_log(DC_LOG_DEBUG, "Sector at LBA %"PRIu64" is slow (%"PRIu64" ms), attempting remap\n",
                   ctx->report.lba, ctx->report.blk_access_time / 1000);

            if (hdparm_remap_sector(ctx->report.lba, ctx->dev->dev_path) == 0) {
                priv->remapped_count++;
                ctx->report.blk_status = DC_BlockStatus_eRemapped; // Mark as remapped
                dc_log(DC_LOG_DEBUG, "Successfully remapped sector %"PRIu64" (total remapped: %"PRIu64")\n",
                       ctx->report.lba, priv->remapped_count);
            } else {
                dc_log(DC_LOG_ERROR, "Failed to remap sector %"PRIu64"\n", ctx->report.lba);
            }
        }
    }

    return ret;
}

static void Close(DC_ProcedureCtx *ctx) {
    ReadRemapPriv *priv = ctx->priv;
    int r = ioctl(priv->fd, BLKRASET, priv->old_readahead);
    if (r == -1)
      dc_log(DC_LOG_WARNING, "Restoring block device readahead setting failed\n");
    free(priv->buf);
    close(priv->fd);

    // Log final remap statistics
    if (priv->remap_enable) {
        dc_log(DC_LOG_INFO, "Read/Remap test completed. Total sectors remapped: %"PRIu64"\n", priv->remapped_count);
    }
}

static const char * const api_choices[] = {"ata", "posix", NULL};
static const char * const remap_enable_choices[] = {"yes", "no", NULL};
static DC_ProcedureOption options[] = {
    { "api", "select operation API: \"posix\" for POSIX read(), \"ata\" for ATA \"READ VERIFY EXT\" command", offsetof(ReadRemapPriv, api_str), DC_ProcedureOptionType_eString, api_choices },
    { "start_lba", "set LBA address to begin from", offsetof(ReadRemapPriv, start_lba), DC_ProcedureOptionType_eInt64 },
    { "remap_enable", "enable automatic sector remapping for slow sectors?", offsetof(ReadRemapPriv, remap_enable_str), DC_ProcedureOptionType_eString, remap_enable_choices },
    { "remap_timeout_ms", "timeout threshold in milliseconds to trigger remap (only used if remapping enabled)", offsetof(ReadRemapPriv, remap_timeout_ms), DC_ProcedureOptionType_eInt64 },
    { NULL }
};


DC_Procedure read_remap_test = {
    .name = "read_remap_test",
    .display_name = "Read/Remap test",
    .help = "Verifies entire device with reading and optionally remaps slow sectors. It reads data sequentially, from given start LBA up to end. Sectors exceeding specified timeout threshold can be automatically remapped using hdparm. To get data from source device, it may use ATA \"READ VERIFY EXT\" command, or POSIX read() function, by user choice.",
    .flags = DC_PROC_FLAG_INVASIVE, // Mark as invasive since it can modify drive
    .suggest_default_value = SuggestDefaultValue,
    .open = Open,
    .perform = Perform,
    .close = Close,
    .priv_data_size = sizeof(ReadRemapPriv),
    .options = options,
};