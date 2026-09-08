//! Recent tracks from every artist the listener follows.

use std::collections::HashSet;
use std::sync::Arc;

use crate::api::ReleaseScanPhase;
use crate::api::models::{Album, PlayableItem};
use crate::app::App;
use crate::model::{Action, Loadable, Page, RowContext, TableItem};
use crate::settings::{ReleaseArtistSource, ReleaseGroup};
use crate::theme::{self, Icon};
use crate::util;

use super::collection::{self, Hero, Table};
use super::widgets;

pub fn show(app: &mut App, ui: &mut egui::Ui) {
    app.ensure_loaded(Page::NewReleases);
    let palette = app.palette;
    let filter_id = egui::Id::new("new-releases-filter");
    let mut filter = ui
        .data(|data| data.get_temp::<String>(filter_id))
        .unwrap_or_default();

    // Keep the page state out of `App` while drawing, like the other
    // collection pages do, so the shared table-row cache can borrow `App`.
    let mut data = std::mem::take(&mut app.new_releases);
    let rows = if let Loadable::Loaded(releases) = &data.releases {
        let groups = app.settings.new_releases_groups.clone();
        let hide_remixes = app.settings.new_releases_hide_remixes;
        let hide_duplicates = app.settings.new_releases_hide_duplicates;
        Some(collection::cached_table_items(
            app,
            Page::NewReleases,
            data.generation,
            data.revision,
            0,
            || release_rows(releases, &groups, hide_remixes, hide_duplicates),
        ))
    } else {
        None
    };
    let release_count = rows
        .as_deref()
        .map(visible_release_count)
        .unwrap_or_default();
    let track_count = rows.as_deref().map(<[TableItem]>::len).unwrap_or_default();
    let mut byline: Vec<(String, Option<Page>)> = app
        .settings
        .new_releases_artist_sources
        .iter()
        .map(|source| {
            let page = match source {
                ReleaseArtistSource::FollowedArtists => Page::Artists,
                ReleaseArtistSource::SavedAlbums => Page::Albums,
                ReleaseArtistSource::LikedSongs => Page::LikedSongs,
            };
            (source.label().to_string(), Some(page))
        })
        .collect();
    byline.push((
        format!("Last {} days", app.settings.new_releases_days),
        None,
    ));
    if rows.is_some() {
        byline.push((
            format!(
                "{} releases, {} songs",
                util::format_count(release_count as u64),
                util::format_count(track_count as u64)
            ),
            None,
        ));
    }
    collection::hero(
        app,
        ui,
        Hero {
            images: Default::default(),
            liked: false,
            kind: "Collection",
            title: "New releases",
            description: Some(
                "Fresh tracks from artists selected from your Spotify library.".into(),
            ),
            byline,
            round: false,
        },
    );

    filters(app, ui, &mut filter, &mut data.minimum_liked_draft);
    ui.data_mut(|data| data.insert_temp(filter_id, filter.clone()));
    ui.add_space(12.0);
    if data.refreshing {
        scan_progress(ui, &data, rows.is_some(), &palette);
        ui.add_space(12.0);
    }

    match (&data.releases, rows) {
        (Loadable::Loading | Loadable::NotLoaded, _) => {
            if !data.refreshing {
                widgets::loading_row(ui, &palette, app.locale);
            }
        }
        (Loadable::Failed(error), _) => {
            widgets::error_row(ui, app, error, Some(Page::NewReleases));
        }
        (Loadable::Loaded(releases), Some(rows)) => {
            if rows.is_empty() {
                let filtering = !releases.is_empty();
                widgets::empty_state(
                    ui,
                    &palette,
                    Icon::Sparkles,
                    if filtering {
                        "No matching releases"
                    } else {
                        "No recent releases"
                    },
                    if filtering {
                        "Try widening the time period or changing the filters."
                    } else {
                        "New tracks from the selected artists will appear here."
                    },
                );
            } else {
                let revision = data.revision;
                let sort = app.table_sorts.get(&Page::NewReleases).copied();
                let view = collection::prepare_table_view(
                    ui,
                    app,
                    &Page::NewReleases,
                    &rows,
                    &filter,
                    sort,
                    revision,
                );
                if view.visible.is_empty() {
                    widgets::empty_state(
                        ui,
                        &palette,
                        Icon::Search,
                        "No matching releases",
                        "Try another artist, track, or album name.",
                    );
                } else {
                    let play_uris: Vec<String> = view
                        .visible
                        .iter()
                        .map(|index| rows[*index].0.uri().to_string())
                        .collect();
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 14.0;
                        if theme::circle_button(
                            ui,
                            Icon::PlayFilled,
                            52.0,
                            palette.accent,
                            palette.accent_hover,
                            palette.on_accent,
                            "Play new releases",
                        )
                        .clicked()
                        {
                            app.actions.push(Action::PlayUris {
                                uris: play_uris,
                                index: 0,
                            });
                        }
                        if theme::icon_button(
                            ui,
                            Icon::Refresh,
                            24.0,
                            palette.secondary,
                            palette.text,
                            "Refresh new releases",
                        )
                        .clicked()
                        {
                            app.actions.push(Action::Reload(Page::NewReleases));
                        }
                    });
                    ui.add_space(12.0);

                    let uris: Arc<[String]> = rows
                        .iter()
                        .map(|(item, _, _)| item.uri().to_string())
                        .collect::<Vec<_>>()
                        .into();
                    collection::table(
                        app,
                        ui,
                        Table {
                            items: &rows,
                            row_offset: 0,
                            pagination: None,
                            context: RowContext::Uris(uris),
                            show_album: true,
                            show_cover: true,
                            show_added: true,
                            added_heading: "RELEASED",
                            show_added_by: false,
                            page: Page::NewReleases,
                            loading: false,
                            error: None,
                            can_load_more: false,
                            filter: &filter,
                            items_revision: revision,
                        },
                    );
                }
            }
        }
        (Loadable::Loaded(_), None) => unreachable!("loaded releases have cached rows"),
    }
    app.new_releases = data;
}

fn scan_progress(
    ui: &mut egui::Ui,
    data: &crate::model::NewReleasesData,
    showing_cached: bool,
    palette: &theme::Palette,
) {
    let prefix = if showing_cached {
        "Refreshing saved results · "
    } else {
        ""
    };
    let Some(progress) = data.progress else {
        ui.horizontal(|ui| {
            theme::spinner(ui, 16.0, palette.accent);
            theme::text(
                ui,
                format!("{prefix}Preparing the release scan…"),
                theme::medium(13.0),
                palette.secondary,
            );
        });
        return;
    };
    if progress.phase == ReleaseScanPhase::CollectingArtists {
        ui.horizontal(|ui| {
            theme::spinner(ui, 16.0, palette.accent);
            theme::text(
                ui,
                format!("{prefix}Collecting artists from your Spotify library…"),
                theme::medium(13.0),
                palette.secondary,
            );
        });
        return;
    }

    let fraction = if progress.total == 0 {
        0.0
    } else {
        progress.completed as f32 / progress.total as f32
    };
    let text = match progress.phase {
        ReleaseScanPhase::CollectingArtists => unreachable!(),
        ReleaseScanPhase::ScanningArtists => format!(
            "{prefix}Checking artist {} of {} · {} releases found",
            progress.completed, progress.total, progress.found_releases
        ),
        ReleaseScanPhase::LoadingTracks => format!(
            "{prefix}Loading tracks for release {} of {}",
            progress.completed, progress.total
        ),
    };
    ui.add(
        egui::ProgressBar::new(fraction.clamp(0.0, 1.0))
            .animate(true)
            .text(text)
            .desired_width(ui.available_width().min(620.0)),
    );
}

fn filters(
    app: &mut App,
    ui: &mut egui::Ui,
    filter: &mut String,
    minimum_liked_draft: &mut Option<u16>,
) {
    let palette = app.palette;
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = egui::vec2(6.0, 6.0);
        theme::text(ui, "Artists from", theme::medium(13.0), palette.secondary);
        for source in ReleaseArtistSource::ALL {
            let selected = app.settings.new_releases_artist_sources.contains(&source);
            if theme::soft_button(ui, &palette, None, source.label(), selected).clicked() {
                app.actions
                    .push(Action::ToggleNewReleaseArtistSource(source));
            }
        }
    });
    if app
        .settings
        .new_releases_artist_sources
        .contains(&ReleaseArtistSource::LikedSongs)
    {
        ui.add_space(6.0);
        ui.horizontal_wrapped(|ui| {
            ui.spacing_mut().item_spacing = egui::vec2(6.0, 6.0);
            theme::text(
                ui,
                "Require a minimum number of songs liked",
                theme::medium(13.0),
                palette.secondary,
            );
            let minimum =
                minimum_liked_draft.get_or_insert(app.settings.new_releases_minimum_liked_tracks);
            let field = ui.add(
                egui::DragValue::new(minimum)
                    .range(1..=u16::MAX)
                    .speed(1)
                    .max_decimals(0),
            );
            let submitted = field.drag_stopped()
                || (field.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter)));
            if submitted || theme::soft_button(ui, &palette, None, "Apply", false).clicked() {
                app.actions
                    .push(Action::SetNewReleasesMinimumLikedTracks(*minimum));
            }
        });
    }
    ui.add_space(6.0);
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = egui::vec2(6.0, 6.0);
        theme::text(ui, "Period", theme::medium(13.0), palette.secondary);
        for (days, label) in [
            (7, "7 days"),
            (30, "30 days"),
            (90, "90 days"),
            (365, "1 year"),
        ] {
            if theme::soft_button(
                ui,
                &palette,
                None,
                label,
                app.settings.new_releases_days == days,
            )
            .clicked()
            {
                app.actions.push(Action::SetNewReleasesDays(days));
            }
        }
    });
    ui.add_space(6.0);
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = egui::vec2(6.0, 6.0);
        theme::text(ui, "Types", theme::medium(13.0), palette.secondary);
        for group in ReleaseGroup::ALL {
            let selected = app.settings.new_releases_groups.contains(&group);
            if theme::soft_button(ui, &palette, None, group.label(), selected).clicked() {
                app.actions.push(Action::ToggleNewReleaseGroup(group));
            }
        }
    });
    ui.add_space(6.0);
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing.x = 16.0;
        let mut hide_remixes = app.settings.new_releases_hide_remixes;
        if ui.checkbox(&mut hide_remixes, "Hide remixes").changed() {
            app.actions
                .push(Action::SetNewReleasesHideRemixes(hide_remixes));
        }
        let mut hide_duplicates = app.settings.new_releases_hide_duplicates;
        if ui
            .checkbox(&mut hide_duplicates, "Hide duplicate releases")
            .changed()
        {
            app.actions
                .push(Action::SetNewReleasesHideDuplicates(hide_duplicates));
        }
        widgets::search_field(
            ui,
            &palette,
            egui::Id::new("new-releases-search"),
            filter,
            "Filter releases",
            220.0,
        );
    });
}

fn visible_release_count(rows: &[TableItem]) -> usize {
    rows.iter()
        .filter_map(|(item, _, _)| match item {
            PlayableItem::Track(track) => track.album.as_ref().map(|album| album.id.as_str()),
            PlayableItem::Episode(_) => None,
        })
        .collect::<HashSet<_>>()
        .len()
}

fn release_rows(
    releases: &[Album],
    groups: &[ReleaseGroup],
    hide_remixes: bool,
    hide_duplicates: bool,
) -> Vec<TableItem> {
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for album in releases {
        if !groups.iter().any(|group| {
            album.album_group.as_deref().or(album.album_type.as_deref())
                == Some(group.spotify_value())
        }) {
            continue;
        }
        if hide_remixes && album.name.to_lowercase().contains("remix") {
            continue;
        }
        if hide_duplicates && !seen.insert(release_identity(album)) {
            continue;
        }
        let Some(tracks) = &album.tracks else {
            continue;
        };
        for track in &tracks.items {
            rows.push((
                PlayableItem::Track(track.clone()),
                album.release_date.clone(),
                None,
            ));
        }
    }
    rows
}

fn release_identity(album: &Album) -> String {
    let title = album
        .name
        .chars()
        .map(|character| match character {
            '[' => '(',
            ']' => ')',
            '’' => '\'',
            other => other,
        })
        .collect::<String>()
        .to_lowercase();
    let artists = album
        .artists
        .iter()
        .map(|artist| artist.name.to_lowercase())
        .collect::<Vec<_>>()
        .join("\0");
    format!(
        "{}\0{artists}\0{title}",
        album.release_date.as_deref().unwrap_or_default()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::{ArtistRef, Page as ApiPage, Track};

    fn release(id: &str, name: &str, group: &str, track_name: &str) -> Album {
        let mut album = Album {
            id: id.into(),
            name: name.into(),
            uri: format!("spotify:album:{id}"),
            album_group: Some(group.into()),
            release_date: Some("2026-09-05".into()),
            artists: vec![ArtistRef {
                name: "Artist".into(),
                ..ArtistRef::default()
            }],
            ..Album::default()
        };
        let summary = album.clone();
        album.tracks = Some(ApiPage {
            items: vec![Track {
                name: track_name.into(),
                uri: format!("spotify:track:{id}"),
                artists: summary.artists.clone(),
                album: Some(summary),
                ..Track::default()
            }],
            total: 1,
            limit: 1,
            ..ApiPage::default()
        });
        album
    }

    #[test]
    fn release_filters_cover_groups_remixes_and_duplicates() {
        let releases = vec![
            release("one", "Fresh [Single]", "single", "Needle Song"),
            release("duplicate", "Fresh (Single)", "single", "Needle Song"),
            release("remix", "Night Remixes", "album", "Another Song"),
        ];
        let groups = ReleaseGroup::ALL;
        assert_eq!(release_rows(&releases, &groups, false, false).len(), 3);
        assert_eq!(release_rows(&releases, &groups, false, true).len(), 2);
        assert_eq!(release_rows(&releases, &groups, true, true).len(), 1);
        assert_eq!(
            release_rows(&releases, &[ReleaseGroup::Single], false, true).len(),
            1
        );
    }
}
