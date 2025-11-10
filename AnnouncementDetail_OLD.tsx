import React, {
  useCallback,
  useEffect,
  useRef,
  useState,
  useMemo,
} from "react";
import {
  View,
  Text,
  StyleSheet,
  Dimensions,
  Animated,
  Easing,
  Platform,
  Alert,
  Linking,
  ActivityIndicator,
  Image,
} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import {
  PanGestureHandler,
  State as GHState,
} from "react-native-gesture-handler";
import AnnouncementSlide from "./AnnouncementSlide";
import { Announcement } from "../types/announcement";
import { getAnnouncement, fetchAnnouncements } from "../api/announcements";
import { formatTimestampToIST } from "../utils/formatTimestamp";

const { height: SCREEN_H } = Dimensions.get("window");

// thresholds and durations
const THRESHOLD = Math.min(150, SCREEN_H * 0.22);
const VELOCITY_THRESHOLD = 0.36;
const HORIZONTAL_BACK_THRESHOLD = 120;
const ANIM_DURATION = 260;

// default slide style used for AnnouncementSlide
const SLIDE_STYLE = {
  height: SCREEN_H,
  justifyContent: "space-between",
  backgroundColor: "#fff",
};

interface RouteParams {
  route?: {
    params?: {
      list?: Announcement[];
      index?: number;
      announcement?: Announcement;
    };
  };
  navigation?: {
    navigate?: (screen: string, params?: any) => void;
    goBack?: () => void;
  };
}

/**
 * AnnouncementDetail
 *
 * Swipeable vertical card stack with persistent triple-slide rendering.
 * Keeps three AnnouncementSlide components mounted permanently to avoid re-mount flicker.
 * Updates props and animates transforms during transitions without unmounting.
 */
export default function AnnouncementDetail({ route, navigation }: RouteParams) {
  // --- route params / initial state
  const params = route?.params || {};
  const initialList: Announcement[] | undefined = params.list;
  const initialIndex: number =
    typeof params.index === "number" ? params.index : 0;
  const initialAnnouncement: Announcement | undefined = params.announcement;

  // --- reactive state
  const [list, setList] = useState<Announcement[]>(initialList ?? []);
  const [currentIndex, setCurrentIndex] = useState<number>(initialIndex);
  const [activeDetail, setActiveDetail] = useState<Announcement | null>(
    initialAnnouncement ?? null,
  );
  const [displayedIndex, setDisplayedIndex] = useState<number>(initialIndex);

  // --- refs for caching, loading, mounted flag, content-ready callbacks
  const cacheRef = useRef<Map<string, Announcement>>(new Map());
  const loadingRef = useRef<Record<string, boolean>>({});
  const mountedRef = useRef(true);
  const contentReadyRef = useRef<Record<string, boolean>>({});
  const contentReadyCallbacksRef = useRef<Record<string, (() => void) | null>>(
    {},
  );

  // --- animation refs
  const translateY = useRef(new Animated.Value(0)).current;
  const translateX = useRef(new Animated.Value(0)).current;
  const overlayOpacity = useRef(new Animated.Value(0)).current;
  const pendingIndexRef = useRef<number | null>(null);
  const animState = useRef<"idle" | "animating">("idle");

  // clamp index utility
  const clampIndex = useCallback(
    (i: number) =>
      list && list.length ? Math.max(0, Math.min(list.length - 1, i)) : 0,
    [list],
  );

  // --- mount / initial fetch
  useEffect(() => {
    mountedRef.current = true;

    // If we don't have an initial list, fetch a page
    if ((!list || list.length === 0) && !initialList) {
      (async () => {
        try {
          const items = await fetchAnnouncements(1, 200);
          if (!mountedRef.current) return;
          setList(items);
          setCurrentIndex(Math.max(0, Math.min(items.length - 1, initialIndex)));
          console.info("AnnouncementDetail: fetched feed items:", items.length);
        } catch (error) {
          console.warn("AnnouncementDetail: feed fetch failed", error);
        }
      })();
    }

    return () => {
      mountedRef.current = false;
    };
  }, [initialList, initialIndex, list]);

  // seed cache with initial announcement (if present)
  useEffect(() => {
    if (initialAnnouncement) {
      cacheRef.current.set(initialAnnouncement.id, initialAnnouncement);
      setActiveDetail(initialAnnouncement);
    }
  }, [initialAnnouncement]);

  // --- load full detail (caching + background refresh)
  const loadDetail = useCallback(
    async (id?: string): Promise<Announcement | null> => {
      if (!id) return null;
      try {
        // if cached, start a background refresh but return cached quickly
        if (cacheRef.current.has(id)) {
          const cached = cacheRef.current.get(id) || null;

          if (!loadingRef.current[id]) {
            loadingRef.current[id] = true;
            (async () => {
              try {
                const full = await getAnnouncement(id);
                if (full && mountedRef.current && animState.current !== "animating") {
                  cacheRef.current.set(id, full);
                  // if that item is currently active, update activeDetail
                  const curIdx = clampIndex(currentIndex);
                  const curId = (list && list[curIdx]?.id) || null;
                  if (curId === id) setActiveDetail(full);
                }
              } catch (error) {
                console.warn("AnnouncementDetail.loadDetail background fetch failed", id, error);
              } finally {
                loadingRef.current[id] = false;
              }
            })();
          }

          return cached;
        }

        // not cached: fetch and cache
        if (loadingRef.current[id]) return null;
        loadingRef.current[id] = true;
        const full = await getAnnouncement(id);
        loadingRef.current[id] = false;
        if (!mountedRef.current) return full;
        if (full) cacheRef.current.set(id, full);
        return full;
      } catch (error) {
        loadingRef.current[id] = false;
        console.warn("AnnouncementDetail.loadDetail failed", error);
        return null;
      }
    },
    [currentIndex, list, clampIndex],
  );

  // --- prefetch neighbouring slides images + details
  const prefetchNeighbors = useCallback(
    (index: number) => {
      if (!list || list.length === 0) return;
      [index - 1, index + 1].forEach((i) => {
        if (i >= 0 && i < list.length) {
          loadDetail(list[i].id)
            .then((full) => {
              // prefetch images if available
              if (full?.banner_image) Image.prefetch(full.banner_image);
              if ((full as any)?.company_logo) Image.prefetch((full as any).company_logo);
            })
            .catch(() => {});
        }
      });
    },
    [list, loadDetail],
  );

  // --- content-ready callbacks (AnnouncementSlide calls onContentReady)
  const onContentReady = useCallback((announcementId: string) => {
    if (!announcementId) return;
    contentReadyRef.current[announcementId] = true;
    const cb = contentReadyCallbacksRef.current[announcementId];
    if (cb) {
      cb();
      contentReadyCallbacksRef.current[announcementId] = null;
    }
  }, []);

  // wait for content ready with a fallback timeout (to avoid stuck overlay)
  const waitForContentReady = useCallback((announcementId: string, timeout = 2000): Promise<void> => {
    return new Promise((resolve) => {
      if (!announcementId) return resolve();
      if (contentReadyRef.current[announcementId]) {
        return resolve();
      }
      contentReadyCallbacksRef.current[announcementId] = resolve;
      // safety fallback
      const t = setTimeout(() => {
        if (contentReadyCallbacksRef.current[announcementId]) {
          contentReadyCallbacksRef.current[announcementId] = null;
        }
        resolve();
      }, timeout);
      // clear fallback if resolved early
      const original = contentReadyCallbacksRef.current[announcementId];
      contentReadyCallbacksRef.current[announcementId] = () => {
        clearTimeout(t);
        original?.();
      };
    });
  }, []);

  // when currentIndex changes, ensure activeDetail is set (summary or cached full)
  useEffect(() => {
    const cur = list?.[currentIndex];
    if (!cur) return;

    if (cacheRef.current.has(cur.id)) {
      setActiveDetail(cacheRef.current.get(cur.id) || cur);
    } else {
      setActiveDetail(null);
    }

    let cancelled = false;
    (async () => {
      const full = await loadDetail(cur.id);
      if (!mountedRef.current || cancelled || animState.current === "animating") return;
      if (full && list[currentIndex]?.id === full.id) setActiveDetail(full);
    })();

    prefetchNeighbors(currentIndex);

    return () => {
      cancelled = true;
    };
  }, [currentIndex, list, loadDetail, prefetchNeighbors]);

  // convenience to read either cached full or summary
  const getDetailOrSummary = useCallback(
    (index: number): Announcement | null => {
      if (!list || list.length === 0) return null;
      const idx = clampIndex(index);
      const summary = list[idx];
      return cacheRef.current.get(summary.id) || summary;
    },
    [list, clampIndex],
  );

  // --- navigation helpers
  const goBackWithIndex = useCallback(
    (idx: number) => {
      try {
        if (navigation?.navigate) navigation.navigate("CardsList" as never, { activeIndex: idx } as never);
        else navigation?.goBack?.();
      } catch {
        navigation?.goBack?.();
      }
    },
    [navigation],
  );

  // open links
  const openUrl = useCallback(async (url?: string) => {
    if (!url) return;
    try {
      const ok = await Linking.canOpenURL(url);
      if (ok) await Linking.openURL(url);
      else Alert.alert("Cannot open URL", url);
    } catch (error) {
      Alert.alert("Error opening URL", String(error));
    }
  }, []);

  const openTradingView = useCallback((url?: string) => void openUrl(url), [openUrl]);
  const openAttachment = useCallback((url?: string) => void openUrl(url), [openUrl]);

  // --- gesture wiring
  const onGestureEvent = Animated.event(
    [{ nativeEvent: { translationY: translateY, translationX: translateX } }],
    { useNativeDriver: true },
  );

  // helper to reset transforms gracefully
  const resetPosition = useCallback((toValue = 0, cb?: () => void) => {
    animState.current = "animating";
    Animated.timing(translateY, {
      toValue,
      duration: ANIM_DURATION,
      easing: Easing.out(Easing.cubic),
      useNativeDriver: true,
    }).start(() => {
      animState.current = "idle";
      translateY.setValue(0);
      translateX.setValue(0);
      cb?.();
    });
  }, [translateX, translateY]);

  // commit to a new index (start the vertical slide animation).
  // overlay decision is done here (only show overlay if we truly have no displayable content).
  // Key fix: defer setCurrentIndex until animation completes to avoid mid-transition re-renders.
  const commitToIndex = useCallback(async (targetIndex: number) => {
    if (targetIndex === currentIndex) return;
    if (animState.current === "animating") return;

    animState.current = "animating";
    const direction = targetIndex > currentIndex ? -1 : 1; // -1 moves up (to higher index)

    // determine displayability
    const targetIdx = clampIndex(targetIndex);
    const summary = list?.[targetIdx] ?? null;
    const targetId = summary?.id ?? null;
    const cachedFull = summary ? (cacheRef.current.get(summary.id) || null) : null;
    const currentActiveFallback = getDetailOrSummary(currentIndex) || activeDetail || null;
    const willHaveDisplay = Boolean(cachedFull || summary || currentActiveFallback);
    const isContentReady = (id?: string) => !!(id && contentReadyRef.current[id]);

    // show overlay only when we do not have any displayable content and content isn't ready
    const shouldShowOverlay = !willHaveDisplay && !isContentReady(targetId);

    console.log("commitToIndex start", { shouldShowOverlay, isReady: isContentReady(targetId), targetId, targetIndex });

    // stop any running overlay animation and ensure consistent state
    try { (overlayOpacity as any).stopAnimation?.(); } catch {}

    if (shouldShowOverlay) {
      // immediately show overlay to mask loading (no fade-in to avoid flicker)
      overlayOpacity.setValue(1);
    } else {
      // ensure overlay hidden synchronously to avoid milky wash
      overlayOpacity.setValue(0);
    }

    // animate the vertical slide
    Animated.timing(translateY, {
      toValue: direction * SCREEN_H,
      duration: ANIM_DURATION,
      easing: Easing.out(Easing.cubic),
      useNativeDriver: true,
    }).start(async () => {
      // Key fix: defer setCurrentIndex until animation fully completes to prevent re-render flicker
      const clampedIndex = clampIndex(targetIndex);
      setCurrentIndex(clampedIndex);
      setDisplayedIndex(clampedIndex);
      pendingIndexRef.current = clampedIndex;

      // reset transform values on next frame to avoid jumps
      requestAnimationFrame(() => {
        translateY.setValue(0);
        translateX.setValue(0);
      });

      // if we didn't show overlay, we are done
      if (!shouldShowOverlay) {
        animState.current = "idle";
        pendingIndexRef.current = null;
        return;
      }

      // Key fix: wait for onContentReady before fading overlay to ensure new slide is visually ready
      await waitForContentReady(targetId || "", 2000);

      // hide overlay with a short fade
      Animated.timing(overlayOpacity, {
        toValue: 0,
        duration: ANIM_DURATION * 0.5,
        useNativeDriver: true,
      }).start(() => {
        pendingIndexRef.current = null;
        animState.current = "idle";
      });
    });
  }, [
    currentIndex,
    list,
    clampIndex,
    getDetailOrSummary,
    activeDetail,
    waitForContentReady,
    translateY,
    translateX,
  ]);

  // when a gesture begins, make sure overlay is stopped and state set properly
  const handleGestureBegin = useCallback(() => {
    try {
      (overlayOpacity as any).stopAnimation?.();
      overlayOpacity.setValue(0);
      pendingIndexRef.current = null;
      animState.current = "idle";
    } catch {
      pendingIndexRef.current = null;
      overlayOpacity.setValue(0);
      animState.current = "idle";
    }
  }, []);

  // handle handler state changes (end/cancel) -> decide whether to commit to another index
  const handleStateChange = useCallback((evt: any) => {
    const { nativeEvent } = evt;
    if (nativeEvent.state === GHState.BEGAN) {
      handleGestureBegin();
      return;
    }

    if ([GHState.END, GHState.CANCELLED, GHState.FAILED].includes(nativeEvent.state)) {
      const {
        translationY: dy = 0,
        velocityY: vy = 0,
        translationX: dx = 0,
      } = nativeEvent;

      // horizontal swipe to go back
      if (Math.abs(dx) > HORIZONTAL_BACK_THRESHOLD && Math.abs(dx) > Math.abs(dy)) {
        goBackWithIndex(currentIndex);
        return;
      }

      const shouldCommitNext = dy < -THRESHOLD || (dy < 0 && Math.abs(vy) > VELOCITY_THRESHOLD);
      const shouldCommitPrev = dy > THRESHOLD || (dy > 0 && Math.abs(vy) > VELOCITY_THRESHOLD);

      if (shouldCommitNext && currentIndex < (list?.length ?? 0) - 1) {
        commitToIndex(currentIndex + 1);
      } else if (shouldCommitPrev && currentIndex > 0) {
        commitToIndex(currentIndex - 1);
      } else {
        resetPosition(0);
      }
    }
  }, [currentIndex, list, commitToIndex, goBackWithIndex, handleGestureBegin, resetPosition]);

  // --- persistent slide data (computed based on currentIndex, updated without re-mounting)
  const prevIndex = useMemo(() => Math.max(0, displayedIndex - 1), [displayedIndex]);
  const nextIndex = useMemo(() => Math.min((list?.length ?? 1) - 1, displayedIndex + 1), [displayedIndex, list]);

  const prevItem = useMemo(() => getDetailOrSummary(prevIndex), [prevIndex, getDetailOrSummary]);
  const currentItem = useMemo(() => {
    const summary = list?.[displayedIndex];
    if (!summary) return null;
    // Prefer activeDetail if IDs match, but fall back to summary data to prevent flicker
    return (activeDetail?.id === summary.id ? activeDetail : summary);
  }, [list, displayedIndex, activeDetail]);
  const nextItem = useMemo(() => getDetailOrSummary(nextIndex), [nextIndex, getDetailOrSummary]);

  // --- precomputed animated transforms for persistent slides (no hooks in render)
  const prevTransform = useMemo(() => ({
    transform: [
      {
        translateY: translateY.interpolate({
          inputRange: [-SCREEN_H, 0, SCREEN_H],
          outputRange: [-SCREEN_H, -SCREEN_H, 0],
          extrapolate: "clamp",
        }),
      },
    ],
    opacity: translateY.interpolate({
      inputRange: [-SCREEN_H, -SCREEN_H / 2, 0, SCREEN_H],
      outputRange: [0, 0.7, 1, 1],
      extrapolate: "clamp",
    }),
    zIndex: 4,
  }), [translateY]);

  const currentTransform = useMemo(() => ({
    transform: [
      {
        translateY: translateY.interpolate({
          inputRange: [-SCREEN_H, 0, SCREEN_H],
          outputRange: [-SCREEN_H, 0, 0],
          extrapolate: "clamp",
        }),
      },
    ],
    opacity: translateY.interpolate({
      inputRange: [-SCREEN_H / 2, -SCREEN_H / 6, 0, SCREEN_H / 6],
      outputRange: [0.88, 0.94, 1, 1],
      extrapolate: "clamp",
    }),
    zIndex: 3,
  }), [translateY]);

  const nextTransform = useMemo(() => ({
    transform: [
      {
        translateY: Animated.add(new Animated.Value(0), Animated.multiply(
          translateY.interpolate({
            inputRange: [-SCREEN_H, 0, SCREEN_H],
            outputRange: [-SCREEN_H, 0, 0],
            extrapolate: "clamp",
          }), 0
        )),
      },
    ],
    zIndex: 2,
  }), [translateY]);

  // active announcement & timestamp for top-left UI
  const activeAnnouncement = currentItem || activeDetail || null;
  const activeTimestamp =
    (activeDetail &&
      (activeDetail.announcement_datetime_human ||
        activeDetail.announcement_datetime_iso ||
        activeDetail.announcement_datetime)) ||
    (activeAnnouncement &&
      (activeAnnouncement.announcement_datetime_human ||
        activeAnnouncement.announcement_datetime_iso ||
        activeAnnouncement.announcement_datetime)) ||
    undefined;

  // --- main render with persistent triple-slide structure
  return (
    <SafeAreaView style={styles.safe}>
      <PanGestureHandler
        onGestureEvent={onGestureEvent}
        onHandlerStateChange={handleStateChange}
      >
        <Animated.View style={styles.container}>
          {/* Overlay that hides underlying content while loading */}
          <Animated.View
            pointerEvents="none"
            style={[
              StyleSheet.absoluteFillObject,
              {
                // subtler but effective mask that sits under top-left UI
                backgroundColor: "rgba(255,255,255,0.85)",
                opacity: overlayOpacity,
                zIndex: 500,   // lower than top-left to avoid covering UI chrome
                elevation: 2,
              },
            ]}
          />

          {/* Persistent triple-slide structure: prev, current, next slides stay mounted */}
          {/* Key fix: stable keys prevent re-mounts, props update on index change */}
          <Animated.View
            key="slide-prev"
            style={[styles.slideWrapper, prevTransform]}
            pointerEvents="none"
          >
            {prevItem ? (
              <AnnouncementSlide
                announcement={prevItem as Announcement}
                marketSnapshot={(prevItem as any)?.market_snapshot ?? null}
                loadingSnapshot={!(prevItem as any)?.market_snapshot}
                onOpenTradingView={openTradingView}
                onOpenAttachment={openAttachment}
                isActive={true} // prev is active for prefetch
                onContentReady={(id) => onContentReady(id)}
                style={SLIDE_STYLE}
              />
            ) : (
              <View style={[styles.loadingContainer, { height: SCREEN_H }]}>
                <ActivityIndicator size="large" />
                <Text style={styles.loadingLabel}>Loading announcement…</Text>
              </View>
            )}
          </Animated.View>

          <Animated.View
            key="slide-current"
            style={[styles.slideWrapper, currentTransform]}
            pointerEvents="auto"
          >
            {currentItem ? (
              <AnnouncementSlide
                announcement={currentItem as Announcement}
                marketSnapshot={(currentItem as any)?.market_snapshot ?? null}
                loadingSnapshot={!(currentItem as any)?.market_snapshot}
                onOpenTradingView={openTradingView}
                onOpenAttachment={openAttachment}
                isActive={true}
                onContentReady={(id) => onContentReady(id)}
                style={SLIDE_STYLE}
              />
            ) : (
              <View style={[styles.loadingContainer, { height: SCREEN_H }]}>
                <ActivityIndicator size="large" />
                <Text style={styles.loadingLabel}>Loading announcement…</Text>
              </View>
            )}
          </Animated.View>

          <Animated.View
            key="slide-next"
            style={[styles.slideWrapper, nextTransform]}
            pointerEvents="none"
          >
            {nextItem ? (
              <AnnouncementSlide
                announcement={nextItem as Announcement}
                marketSnapshot={(nextItem as any)?.market_snapshot ?? null}
                loadingSnapshot={!(nextItem as any)?.market_snapshot}
                onOpenTradingView={openTradingView}
                onOpenAttachment={openAttachment}
                isActive={true}
                onContentReady={(id) => onContentReady(id)}
                style={SLIDE_STYLE}
              />
            ) : null}
          </Animated.View>
        </Animated.View>
      </PanGestureHandler>
    </SafeAreaView>
  );
}

// --- styles
const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: "#00000000" },
  container: { flex: 1, backgroundColor: "#fff", overflow: "hidden" },
  slideWrapper: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    height: SCREEN_H,
    backgroundColor: "#f6f6f7", // neutral fallback while content/images paint
    shadowColor: "#000",
    shadowOpacity: Platform.OS === "ios" ? 0.12 : 0.18,
    shadowRadius: 18,
    shadowOffset: { width: 0, height: 8 },
    elevation: 6,
    overflow: "hidden",
  },
  loadingContainer: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 16,
  },
  loadingLabel: { color: "#6b7280", marginTop: 12 },
});