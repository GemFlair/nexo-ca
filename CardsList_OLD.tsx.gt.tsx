// NEXIS - src/screens/CardsList.tsx
// Screen-1: Announcement feed for NEXIS — rebuilt: mint-under-text + datetime fallback + fade+scale animations

import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  View,
  Text,
  Image,
  FlatList,
  StyleSheet,
  ActivityIndicator,
  Platform,
  RefreshControl,
  Animated,
  Alert,
  TouchableOpacity,
} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import AsyncStorage from "@react-native-async-storage/async-storage";

import { fetchAnnouncements } from "../api/announcements";
import { Announcement } from "../types/announcement";
import { formatTimestampToIST } from "../utils/formatTimestamp";

/** Keep the original AsyncStorage key so existing viewed marks remain valid */
const VIEWED_KEY = "nexis_viewed_ids_v3_1.0.0";

/** Safe logo URL using centralized API base */
const safeLogoUri = (logo?: string | null): string | undefined => {
  if (!logo) return undefined;
  const trimmed = String(logo).trim();
  if (!trimmed) return undefined;
  if (/^https?:\/\//i.test(trimmed)) return trimmed;
  const base = (global as any).API_BASE
    ? String((global as any).API_BASE).replace(/\/+$/, "")
    : "http://127.0.0.1:8000";
  return `${base}${trimmed.startsWith("/") ? trimmed : `/${trimmed}`}`;
};

/** Pick best datetime field from backend item */
const pickDatetimeRaw = (item: Announcement): string | undefined => {
  return (
    item.announcement_datetime ||
    (item as any).announcement_datetime_human ||
    (item as any).announcement_datetime_iso ||
    (item as any).announcement_datetime_text ||
    (item as any).announcement_datetime_raw ||
    undefined
  );
};

/** Render datetime: prefer parsed IST string, otherwise show raw or dash */
const renderDatetime = (raw?: string | undefined): string => {
  if (!raw) return "—";
  try {
    const parsed = new Date(String(raw));
    if (!isNaN(parsed.getTime()))
      return formatTimestampToIST(parsed.toISOString());
  } catch {}
  return String(raw).trim() || "—";
};

/** Sentiment badge (keeps your colors/labels) */
const SentimentBadge: React.FC<{ label?: string }> = ({ label }) => {
  const raw = (label ?? "").toString();
  const v = raw.trim().toLowerCase();

  let bg = "#f0ad4e"; // Neutral
  let display = "Neutral";

  if (v === "positive" || v.includes("posit") || v === "pos") {
    bg = "#2ecc71";
    display = "Positive";
  } else if (v === "negative" || v.includes("negat") || v === "neg") {
    bg = "#e74c3c";
    display = "Negative";
  } else if (v === "ambiguous" || v === "mixed" || v === "uncertain") {
    bg = "#3490dc";
    display = "Ambiguous";
  }

  return (
    <View style={[styles.badge, { backgroundColor: bg }]}>
      <Text style={styles.badgeText}>{display}</Text>
    </View>
  );
};

/**
 * Card row — preserves original structure/typography while adding:
 *  - mint green UNDER text (base underlay)
 *  - white overlay that fades IN when viewed (so mint -> white)
 *  - press scale micro-interaction
 */
const CardRow: React.FC<{
  item: Announcement;
  onPress?: (id: string, item?: Announcement) => void;
  viewed?: boolean;
}> = ({ item, onPress, viewed }) => {
  const logoUri = safeLogoUri(item.company_logo ?? undefined);
  const companyName = item.company_name ?? "Unknown Company";
  const headline = item.headline ?? "";

  // white overlay: 0 -> transparent (mint visible), 1 -> opaque white (viewed)
  const whiteOverlay = useRef(new Animated.Value(viewed ? 1 : 0)).current;
  const scale = useRef(new Animated.Value(1)).current;

  // animate overlay when viewed toggles
  useEffect(() => {
    Animated.timing(whiteOverlay, {
      toValue: viewed ? 1 : 0,
      duration: 300,
      useNativeDriver: false, // opacity on View; safe and compatible
    }).start();
  }, [viewed, whiteOverlay]);

  // press micro-interaction
  const handlePressIn = () => {
    Animated.spring(scale, { toValue: 0.97, useNativeDriver: true }).start();
  };
  const handlePressOut = () => {
    Animated.spring(scale, {
      toValue: 1,
      friction: 4,
      tension: 50,
      useNativeDriver: true,
    }).start();
  };

  return (
    <Animated.View style={[styles.cardWrapper, { transform: [{ scale }] }]}>
      {/* Mint underlay: positioned below everything */}
      <View
        style={[
          StyleSheet.absoluteFillObject,
          { backgroundColor: "#ecfdf5", borderRadius: 10 },
        ]}
      />

      {/* White overlay: fades in on viewed — sits above mint but below text because content is transparent */}
      <Animated.View
        pointerEvents="none"
        style={[
          StyleSheet.absoluteFillObject,
          {
            backgroundColor: "#ffffff",
            borderRadius: 10,
            opacity: whiteOverlay,
          },
        ]}
      />

      {/* Content container: transparent background so mint shows when overlay is transparent */}
      <TouchableOpacity
        activeOpacity={0.85}
        onPress={() => onPress && item.id && onPress(item.id, item)}
        onPressIn={handlePressIn}
        onPressOut={handlePressOut}
        style={styles.cardContent}
      >
        <View style={styles.companyRow}>
          <Text
            numberOfLines={1}
            ellipsizeMode="tail"
            style={styles.companyName}
          >
            {companyName}
          </Text>
          {logoUri ? (
            <Image
              source={{ uri: logoUri }}
              style={styles.companyLogo}
              resizeMode="contain"
            />
          ) : (
            <View style={styles.logoPlaceholder} />
          )}
        </View>

        <Text style={styles.headline} numberOfLines={3} ellipsizeMode="tail">
          {headline}
        </Text>

        <View style={styles.metaRow}>
          <Text style={styles.dateText} numberOfLines={1}>
            {renderDatetime(pickDatetimeRaw(item))}
          </Text>
          <SentimentBadge
            label={
              item.sentiment_final?.label ||
              (item as any).sentiment ||
              (item as any).sentiment_badge?.label
            }
          />
        </View>
      </TouchableOpacity>
    </Animated.View>
  );
};

/** Root component — keeps your original data-loading and navigation behavior */
const CardsListScreen: React.FC<{ navigation?: any }> = ({ navigation }) => {
  const [items, setItems] = useState<Announcement[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [viewedTimestamps, setViewedTimestamps] = useState<
    Record<string, string>
  >({});

  const page = 1;

  const loadViewed = useCallback(async () => {
    try {
      const raw = await AsyncStorage.getItem(VIEWED_KEY);
      if (!raw) {
        setViewedTimestamps({});
        return;
      }
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") setViewedTimestamps(parsed);
      else setViewedTimestamps({});
    } catch (e) {
      console.warn("loadViewed error", e);
      setViewedTimestamps({});
    }
  }, []);

  const saveViewed = useCallback(async (obj: Record<string, string>) => {
    try {
      await AsyncStorage.setItem(VIEWED_KEY, JSON.stringify(obj));
    } catch (e) {
      console.warn("saveViewed error", e);
    }
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    const data = await fetchAnnouncements(page, 50);
    setItems(data);
    setLoading(false);
  }, [page]);

  useEffect(() => {
    loadViewed();
    load();
  }, [load, loadViewed]);

  const onPressItem = useCallback(
    async (id: string, item?: Announcement) => {
      if (!id) return;
      try {
        const ts = new Date().toISOString();
        const next = { ...viewedTimestamps, [id]: ts };
        setViewedTimestamps(next);
        await saveViewed(next);
      } catch (e) {
        console.warn("onPressItem save viewed failed", e);
      }
      if (navigation && navigation.navigate) {
        navigation.navigate(
          "AnnouncementDetail" as never,
          {
            announcement: item,
            list: items,
            index:
              items && Array.isArray(items)
                ? items.findIndex((x) => x.id === id)
                : 0,
          } as never,
        );
      } else {
        // fallback

        Alert.alert("Open Detail", `Open detail for ${id}`);
      }
    },
    [navigation, viewedTimestamps, saveViewed, items],
  );

  const onRefresh = useCallback(async () => {
    setRefreshing(true);
    const data = await fetchAnnouncements(page, 50);
    setItems(data);
    setRefreshing(false);
  }, [page]);

  if (loading) {
    return (
      <SafeAreaView style={styles.center}>
        <ActivityIndicator size="large" />
        <Text style={styles.loadingText}>Loading announcements...</Text>
      </SafeAreaView>
    );
  }

  const list = items && Array.isArray(items) ? items : [];

  if (list.length === 0) {
    return (
      <SafeAreaView style={styles.center}>
        <Text style={styles.emptyText}>No announcements found.</Text>
      </SafeAreaView>
    );
  }

  return (
    <View style={{ flex: 1 }}>
      {/* Top safe area (status bar) + header: make the safe area itself blue */}
      <SafeAreaView edges={["top"]} style={{ backgroundColor: "#1E40AF" }}>
        <View style={styles.headerBar}>
          <Text style={styles.headerTitle}>
            NSE/BSE Corporate Announcements
          </Text>
        </View>
      </SafeAreaView>

      {/* Main content area */}
      <View style={styles.container}>
        <FlatList
          data={list}
          keyExtractor={(item) =>
            item.id ?? `${item.company_name ?? "no-name"}-${Math.random()}`
          }
          renderItem={({ item }) => (
            <CardRow
              item={item}
              onPress={onPressItem}
              viewed={!!viewedTimestamps[item.id]}
            />
          )}
          contentContainerStyle={[styles.listContainer, { paddingTop: 12 }]}
          ItemSeparatorComponent={() => <View style={styles.separator} />}
          initialNumToRender={10}
          removeClippedSubviews={true}
          refreshControl={
            <RefreshControl refreshing={refreshing} onRefresh={onRefresh} />
          }
        />
      </View>
    </View>
  );
};

export default CardsListScreen;

/** Styles */
const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: "#f8fafc" },
  listContainer: { paddingVertical: 8, paddingHorizontal: 12 },
  cardWrapper: {
    borderRadius: 10,
    overflow: "hidden",
    marginBottom: 8,
    // shadow/elevation are applied on wrapper; background is transparent so mint underlay shows
    shadowColor: "#000",
    shadowOpacity: Platform.OS === "ios" ? 0.22 : 0.28,
    shadowOffset: { width: 0, height: 4 },
    shadowRadius: 8,
    elevation: 8,
  },
  /* Note: cardContent is transparent — mint underlay + white overlay control appearance */
  cardContent: {
    padding: 12,
    borderRadius: 10,
    backgroundColor: "transparent",
  },
  card: {
    backgroundColor: "#ffffff",
    borderRadius: 10,
    padding: 12,
  },
  unviewedCard: { backgroundColor: "#ecfdf5" }, // kept for compatibility but not used in new flow
  companyRow: { flexDirection: "row", alignItems: "center", marginBottom: 8 },
  companyName: {
    fontSize: 16,
    fontWeight: "700",
    color: "#111827",
    marginRight: 0,
  },
  companyLogo: { width: 24, height: 24, marginLeft: 8 },
  logoPlaceholder: {
    width: 20,
    height: 20,
    marginLeft: 8,
    borderRadius: 3,
    backgroundColor: "#eee",
  },
  headline: { fontSize: 14, color: "#333", marginBottom: 8, lineHeight: 18 },
  metaRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  dateText: { fontSize: 12, fontStyle: "italic", color: "#6b7280" },
  badge: { paddingHorizontal: 8, paddingVertical: 4, borderRadius: 8 },
  badgeText: { color: "#000", fontSize: 12, fontWeight: "500" },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 24,
  },
  loadingText: { marginTop: 12, color: "#6b7280" },
  emptyText: { color: "#6b7280" },
  separator: { height: 4 },
  headerBar: {
    backgroundColor: "#1E40AF", // deep blue
    paddingTop: Platform.OS === "android" ? 20 : 0, // extra top space for Android status bar
    paddingBottom: 12,
    alignItems: "center",
    justifyContent: "center",
  },
  headerTitle: { color: "#ffffff", fontSize: 18, fontWeight: "600" },
});
