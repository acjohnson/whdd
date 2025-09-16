#include <inttypes.h>
#include "vis.h"
#include "procedure.h"

vis_t bs_vis[]   = {
                    { 3000,   L'\u2588', 0, MY_COLOR_BLUE }, // blue block
                    { 10000,  L'\u2588', 1, MY_COLOR_YELLOW }, // yellow block
                    { 50000,  L'\u2591', 1, MY_COLOR_GREEN }, // green dark shade
                    { 150000, L'\u2588', 0, MY_COLOR_GREEN }, // green full block
                    { 500000, L'\u2588', 0, MY_COLOR_RED }, // red full block
};
vis_t exceed_vis =  { 0,      L'\u2588', 1, MY_COLOR_RED }; // bold red full block
vis_t error_vis[]= {
                    { 0,      L' ',      1, MY_COLOR_RED }, // unused
                    { 0,      L'*',      1, MY_COLOR_RED }, // eError
                    { 0,      L'?',      1, MY_COLOR_GRAY }, // eTimeout
                    { 0,      L'x',      1, MY_COLOR_RED }, // eUnc
                    { 0,      L'S',      1, MY_COLOR_GREEN }, // eIdnf
                    { 0,      L'!',      1, MY_COLOR_RED }, // eAbrt
                    { 0,      L'A',      0, MY_COLOR_ORANGE }, // eAmnf
                    { 0,      L'R',      1, MY_COLOR_CYAN }, // eRemapped
};
// Note: remapped_vis removed - use error_vis[DC_BlockStatus_eRemapped] instead
void init_my_colors(void) {
    init_pair(MY_COLOR_GRAY, COLOR_WHITE, COLOR_BLACK);
    init_pair(MY_COLOR_GREEN, COLOR_GREEN, COLOR_BLACK);
    init_pair(MY_COLOR_RED, COLOR_RED, COLOR_BLACK);
    init_pair(MY_COLOR_WHITE_ON_BLUE, COLOR_WHITE, COLOR_BLUE);
    init_pair(MY_COLOR_ORANGE, COLOR_YELLOW, COLOR_BLACK);
    init_pair(MY_COLOR_BLUE, COLOR_BLUE, COLOR_BLACK);
    init_pair(MY_COLOR_YELLOW, COLOR_YELLOW, COLOR_BLACK);
    init_pair(MY_COLOR_CYAN, COLOR_CYAN, COLOR_BLACK);
}

vis_t choose_vis(uint64_t access_time) {
    unsigned int i;
    for (i = 0; i < sizeof(bs_vis)/sizeof(*bs_vis); i++)
        if (access_time < bs_vis[i].access_time)
            return bs_vis[i];
    return exceed_vis;
}


void print_vis(WINDOW *win, vis_t vis) {
    if (vis.attrs)
        wattron(win, A_BOLD);
    else
        wattroff(win, A_BOLD);
    wattron(win, COLOR_PAIR(vis.color_pair));
    wprintw(win, "%lc", vis.vis);
}

void show_legend(WINDOW *win, int show_remap) {
    unsigned int i;
    for (i = 0; i < sizeof(bs_vis)/sizeof(*bs_vis); i++) {
        print_vis(win, bs_vis[i]);
        wattrset(win, A_NORMAL);
        wprintw(win, " <%"PRIu64"ms\n", bs_vis[i].access_time / 1000);
    }
    print_vis(win,exceed_vis);
    wattrset(win, A_NORMAL);
    wprintw(win, " >500ms\n");

    print_vis(win, error_vis[1]);
    wattrset(win, A_NORMAL);
    wprintw(win, " ERR\n");

    print_vis(win, error_vis[2]);
    wattrset(win, A_NORMAL);
    wprintw(win, " TIME\n");

    print_vis(win, error_vis[3]);
    wattrset(win, A_NORMAL);
    wprintw(win, " UNC\n");

    print_vis(win, error_vis[4]);
    wattrset(win, A_NORMAL);
    wprintw(win, " IDNF\n");

    print_vis(win, error_vis[5]);
    wattrset(win, A_NORMAL);
    wprintw(win, " ABRT\n");

    print_vis(win, error_vis[6]);
    wattrset(win, A_NORMAL);
    wprintw(win, " AMNF\n");

    if (show_remap) {
        print_vis(win, error_vis[DC_BlockStatus_eRemapped]);
        wattrset(win, A_NORMAL);
        wprintw(win, " REMAP\n");
    }

    wrefresh(win);
}

