// frontend/src/screens/AnnouncementSlide.tsx
// Final polished version — inline logo after company name, banner handling + 3x3 snapshot.

import React, { useCallback, useEffect, useRef, memo, useState } from "react";
import {
  View,
  Text,
  Image,
  TouchableOpacity,
  StyleSheet,
  Dimensions,
  Animated,
  AccessibilityInfo,
  ActivityIndicator,
} from "react-native";
import { Announcement } from "../types/announcement";
import { formatTimestampToIST } from "../utils/formatTimestamp";
import { sanitizeHtml } from "../utils/sanitizeHtml";

const { height: SCREEN_H } = Dimensions.get("window");
const BANNER_H = Math.min(260, Math.max(SCREEN_H * 0.25, 180));

/**
 * Robust public URI picker
 * - Prefer http(s) or '/static/...'
 * - If FS path contains processed_images, map to /static/images/processed_images/...
 * - Ensure /static/... paths are prefixed with API_BASE when necessary
 */
const pickPublicUri = (input: any): string | undefined => {
  if (!input && input !== 0) return undefined;

  const base =
    (global as any).API_BASE && typeof (global as any).API_BASE === "string"
      ? String((global as any).API_BASE).replace(/\/+$/, "")
      : "http://127.0.0.1:8000";

  const mapPath = (path: string, prefix: string): string => {
    const after = path.slice(path.indexOf(prefix) + prefix.length);
    return `${base}/static/images/${prefix}${after}`
      .replace(/\/{2,}/g, "/")
      .replace("http:/", "http://");
  };

  const tryOne = (v: any): string | undefined => {
    if (!v && v !== 0) return undefined;
    if (Array.isArray(v)) {
      for (const a of v) {
        const p = tryOne(a);
        if (p) return p;
      }
      return undefined;
    }
    const s = String(v).trim();
    if (!s) return undefined;
    if (/^https?:\/\//i.test(s)) return s; // Absolute URL
    if (s.includes("/static/"))
      return `${base}${s.slice(s.indexOf("/static/"))}`;
    if (s.includes("/processed_images/"))
      return mapPath(s, "processed_images/");
    if (s.includes("processed_banners/"))
      return mapPath(s, "processed_banners/");
    if (s.includes("processed_logos/")) return mapPath(s, "processed_logos/");
    if (s.startsWith("/")) return `${base}${s}`;
    return `${base}/${s}`;
  };

  return tryOne(input);
};

function plainTextFromHtml(html?: string | null): string {
  if (!html) return "";
  try {
    const s = sanitizeHtml(html);
    return s.replace(/<\/?[^>]+(>|$)/g, "").trim();
  } catch {
    return String(html ?? "");
  }
}

function tolerantFormatDate(raw?: string | null): string {
  if (!raw) return "—";
  const s = String(raw).trim();
  if (!s) return "—";
  try {
    const parsed = new Date(s);
    if (!isNaN(parsed.getTime()))
      return formatTimestampToIST(parsed.toISOString());
  } catch {}
  return s;
}

const SentimentBadge: React.FC<{ label?: string }> = ({ label }) => {
  const raw = (label ?? "").toString();
  const v = raw.trim().toLowerCase();

  let bg = "#f0ad4e";
  let display = label ?? "Neutral";

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

// Inline definition of Props type
interface Props {
  announcement: Announcement;
  marketSnapshot?: Record<string, any> | null;
  loadingSnapshot?: boolean;
  onOpenTradingView?: (url?: string) => void;
  onOpenAttachment?: (url?: string) => void;
  isActive?: boolean;
  onContentReady?: (announcementId: string) => void;
  style?: Record<string, any>;
}


const AnnouncementSlideInner: React.FC<Props> = ({
  announcement,
  marketSnapshot,
  loadingSnapshot,
  onOpenTradingView,
  onOpenAttachment,
  isActive,
  onContentReady,
  style,
}) => {
  // Only log when slide becomes active
  useEffect(() => {
    if (isActive) {
      console.log('AnnouncementSlide activated', { id: announcement?.id });
    }
  }, [isActive, announcement?.id]);

  const fadeAnim = useRef(new Animated.Value(isActive ? 1 : 0)).current;
  const [bannerError, setBannerError] = useState(false);
  const imgOpacity = useRef(new Animated.Value(0)).current;
  const [imagesLoaded, setImagesLoaded] = useState({ banner: false, logo: false });
  const firedRef = useRef(false);

  useEffect(() => {
    AccessibilityInfo.isReduceMotionEnabled().then((r) => {
      if (r) fadeAnim.setValue(1);
      else {
        Animated.timing(fadeAnim, {
          toValue: isActive ? 1 : 0.5,
          duration: 250,
          useNativeDriver: true,
        }).start();
      }
    });
  }, [isActive, fadeAnim]);

  const onBannerLoad = () => {
    if (firedRef.current) return;
    firedRef.current = true;
    // crossfade animation
    Animated.timing(imgOpacity, { 
      toValue: 1, 
      duration: 180, 
      useNativeDriver: true 
    }).start(() => {
      onContentReady?.(announcement?.id);
      requestAnimationFrame(() => onContentReady?.(announcement?.id));
    });
    setImagesLoaded((s) => ({ ...s, banner: true }));
  };

  const onLogoLoad = () => setImagesLoaded((s) => ({ ...s, logo: true }));

  const ann = React.useMemo(
    () => announcement ?? ({} as Announcement),
    [announcement],
  );
  const vendorSnapshot = marketSnapshot || (ann as any).market_snapshot || null;

  // prefer market_snapshot.banner_url -> ann.banner_image
  const bannerCandidates =
    vendorSnapshot?.banner_url ?? ann.banner_image ?? undefined;
  let bannerUri = pickPublicUri(bannerCandidates);
  if (!bannerUri && vendorSnapshot?.logo_url)
    bannerUri = pickPublicUri(vendorSnapshot.logo_url);

  const logoCandidates =
    vendorSnapshot?.logo_url ?? ann.company_logo ?? undefined;
  const logoUri = pickPublicUri(logoCandidates);

  const company = ann.company_name ?? "Unknown Company";
  const symbol = ann.symbol ?? "";
  const summary = ann.summary_60 ?? "—";
  const body = plainTextFromHtml(
    (ann as any).body ?? (ann as any).summary_raw ?? "",
  );
  const dtRaw =
    (ann as any).announcement_datetime_human ??
    (ann as any).announcement_datetime_iso ??
    (ann as any).announcement_datetime ??
    (ann as any).timestamp ??
    null;
  const dtFormatted = tolerantFormatDate(dtRaw);

  const snapshot = vendorSnapshot;
  const snapshotTsRaw =
    snapshot?.market_snapshot_date ??
    snapshot?.snapshot_date ??
    snapshot?.as_of ??
    snapshot?.last_updated ??
    null;
  const snapshotTsLabel = snapshotTsRaw
    ? tolerantFormatDate(snapshotTsRaw)
    : "—";

  const tradingUrl = ann.tradingview_url ?? undefined;

  // attachments (memoized)
  const attachments = React.useMemo(() => {
    const raw = (ann as any).attachments;
    if (!Array.isArray(raw) || raw.length === 0) return [] as any[];
    return raw.filter(
      (a: any) => a && typeof a.url === "string" && a.url.trim().length > 0,
    );
  }, [ann]);
  const pdfUrl =
    attachments.find((a: any) => a.url?.toLowerCase().endsWith(".pdf"))?.url ??
    (ann as any).source_file ??
    (ann as any).source_file_original ??
    undefined;

  // If needed in future, use openPdfOrAttachment to open attachments

  // metrics mapping
  const price = snapshot?.price ?? null;
  const change24 =
    snapshot?.change_1d_pct ??
    snapshot?.change_percent ??
    snapshot?.change_percent_1d ??
    snapshot?.change24 ??
    null;
  const change7d =
    snapshot?.change_1w_pct ??
    snapshot?.change_week_pct ??
    snapshot?.change7d ??
    null;
  const rank = snapshot?.rank ?? null;
  const mcap = snapshot?.mcap_rs_cr ?? snapshot?.mcap ?? null;
  const vol24 = snapshot?.volume_24h_rs_cr ?? snapshot?.volume_24h ?? null;
  const volChange24 =
    snapshot?.vol_change_pct ?? snapshot?.volume_change_pct ?? null;
  const atr = snapshot?.atr_pct ?? snapshot?.atr ?? null;
  const vwap = snapshot?.vwap ?? null;

  const fmtNum = (v: any, digits = 2) =>
    v != null && isFinite(Number(v))
      ? Number(v).toFixed(digits)
      : typeof v === "string"
        ? v
        : "—";
  const tri = (v?: number | null) => {
    if (v == null) return "";
    const n = Number(v);
    if (isNaN(n)) return "";
    if (n > 0) return "▲";
    if (n < 0) return "▼";
    return "";
  };

  const LogoNode = () =>
    logoUri ? (
      <Image
        source={{ uri: logoUri }}
        style={styles.smallLogo}
        resizeMode="contain"
        onLoad={onLogoLoad}
      />
    ) : (
      <View style={styles.logoPlaceholder}>
        <Text style={styles.logoPlaceholderText}>
          {(company || "C").slice(0, 2).toUpperCase()}
        </Text>
      </View>
    );

  const handleImageError = useCallback(() => {
    setBannerError(true);
    console.warn("AnnouncementSlide: banner image failed to load:", bannerUri);
  }, [bannerUri]);

  return (
    <Animated.View style={[styles.container, { opacity: fadeAnim }, style]}>
      {/* Banner area with skeleton placeholder */}
      <View style={styles.topSection}>
        {/* Animated image positioned absolute over the skeleton */}
        {!bannerError && bannerUri ? (
          <Animated.Image
            source={{ uri: bannerUri }}
            style={[
              styles.banner,
              {
                position: 'absolute',
                top: 0,
                left: 0,
                right: 0,
                bottom: 0,
                opacity: imgOpacity,
              },
            ]}
            resizeMode="cover"
            onLoad={onBannerLoad}
            onError={() => {
              setBannerError(true);
              if (!firedRef.current) {
                firedRef.current = true;
                onContentReady?.(announcement?.id);
              }
            }}
          />
        ) : null}
      </View>

      {/* Content */}
      <View style={styles.contentArea}>
        {/* header row: make name + logo inline */}
        <View style={styles.headerRow}>
          <View
            style={[
              styles.headerLeft,
              { flexDirection: "row", alignItems: "center" },
            ]}
          >
            <Text style={styles.companyHeaderText}>
              {symbol ? `${symbol} | ${company}` : company}
            </Text>
            <View style={{ width: 6 }} />
            <LogoNode />
          </View>
        </View>

        {/* subtext below name */}
        <Text style={styles.companySubText}>
          {snapshot?.broad_index ?? ann.broad_index ?? "—"} |{" "}
          {snapshot?.sector_index ?? ann.sector_index ?? "—"}
        </Text>

        {/* Datetime + sentiment inline */}
        <View style={styles.dateSentimentRow}>
          <Text style={styles.announcementDateText}>
            Announcement: {dtFormatted ?? "—"}
          </Text>
          <View style={{ width: 12 }} />
          {ann.sentiment_final?.label ||
          ann.sentiment_label ||
          ann.sentiment ? (
            <View style={{ justifyContent: "center" }}>
              <SentimentBadge
                label={
                  ann.sentiment_final?.label ||
                  ann.sentiment_label ||
                  ann.sentiment
                }
              />
            </View>
          ) : null}
        </View>

        {/* Summary (justified) */}
        <Text style={styles.summary}>{summary}</Text>

        {/* Body (justified) */}
        {body ? <Text style={styles.body}>{body}</Text> : null}

        {/* Market Snapshot card */}
        <View style={styles.snapshotCard}>
          {loadingSnapshot ? (
            <ActivityIndicator />
          ) : snapshot ? (
            <>
              <Text style={styles.snapshotHeader}>
                𝗠𝗮𝗿𝗸𝗲𝘁 𝗦𝗻𝗮𝗽𝘀𝗵𝗼𝘁: | {snapshotTsLabel} , EOD |
              </Text>

              {/* Row 1 */}
              <View style={styles.gridRow}>
                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>Price</Text>
                  <Text style={styles.gridValue}>₹{fmtNum(price)}</Text>
                </View>

                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>Change (24H)</Text>
                  <Text
                    style={[
                      styles.gridValue,
                      change24 > 0
                        ? styles.positive
                        : change24 < 0
                          ? styles.negative
                          : {},
                    ]}
                  >
                    {fmtNum(change24)}% {tri(change24)}
                  </Text>
                </View>

                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>Change (7D)</Text>
                  <Text
                    style={[
                      styles.gridValue,
                      change7d > 0
                        ? styles.positive
                        : change7d < 0
                          ? styles.negative
                          : {},
                    ]}
                  >
                    {fmtNum(change7d)}% {tri(change7d)}
                  </Text>
                </View>
              </View>

              {/* Row 2 */}
              <View style={[styles.gridRow, { marginTop: 8 }]}>
                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>Rank</Text>
                  <Text style={styles.gridValue}>{rank ?? "—"}</Text>
                </View>
                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>MCap</Text>
                  <Text style={styles.gridValue}>₹{fmtNum(mcap)} Cr</Text>
                </View>
                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>Volume (24H)</Text>
                  <Text style={styles.gridValue}>₹{fmtNum(vol24)} Cr</Text>
                </View>
              </View>

              {/* Row 3 */}
              <View style={[styles.gridRow, { marginTop: 8 }]}>
                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>Vol Change (24H)</Text>
                  <Text
                    style={[
                      styles.gridValue,
                      volChange24 > 0
                        ? styles.positive
                        : volChange24 < 0
                          ? styles.negative
                          : {},
                    ]}
                  >
                    {fmtNum(volChange24)}% {tri(volChange24)}
                  </Text>
                </View>
                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>ATR (14D)</Text>
                  <Text style={styles.gridValue}>{fmtNum(atr)}%</Text>
                </View>
                <View style={styles.gridCol}>
                  <Text style={styles.gridLabel}>VWAP (24H)</Text>
                  <Text style={styles.gridValue}>₹{fmtNum(vwap)}</Text>
                </View>
              </View>
            </>
          ) : (
            <Text style={styles.snapshotEmpty}>No market data available</Text>
          )}
        </View>

        {/* TradingView (kept) */}
        {tradingUrl && (
          <TouchableOpacity
            onPress={() => onOpenTradingView?.(tradingUrl)}
            style={styles.tradeBtn}
            accessibilityRole="button"
            accessibilityLabel="View on TradingView"
          >
            <Text style={styles.tradeText}>📈 View on TradingView</Text>
          </TouchableOpacity>
        )}
        {(pdfUrl || (attachments && attachments.length > 0)) && (
          <TouchableOpacity
            onPress={() => onOpenAttachment?.(pdfUrl ?? attachments[0].url)}
            style={[styles.tradeBtn, { backgroundColor: "#111827" }]}
            accessibilityRole="button"
            accessibilityLabel="Open attachment"
          >
            <Text style={[styles.tradeText, { color: "#fff" }]}>
              Open attachment
            </Text>
          </TouchableOpacity>
        )}
      </View>
      {/* bottom colored bar to highlight card edge while swiping */}
      <View pointerEvents="none" style={styles.bottomBar} />
    </Animated.View>
  );
};

const areEqual = (prevProps: Props, nextProps: Props) => {
  if (prevProps.announcement === nextProps.announcement) {
    // Same announcement object - only re-render if becoming active
    return !(nextProps.isActive && !prevProps.isActive);
  } else {
    // Different announcement object
    if (prevProps.announcement?.id === nextProps.announcement?.id) {
      // Same ID but different object (e.g., summary to full) - allow re-render for data updates
      return false;
    }
    // Different ID - only render if becoming active
    return !nextProps.isActive;
  }
};

const AnnouncementSlide = React.memo(AnnouncementSlideInner, areEqual);
export default AnnouncementSlide;

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#000000ff",
    borderRadius: 20,
    overflow: "hidden",
  },
  bottomBar: {
    position: "absolute",
    left: 0,
    right: 0,
    bottom: 0,
    height: 24,
    backgroundColor: "#000000e2",
    borderBottomLeftRadius: 20,
    borderBottomRightRadius: 20,
    zIndex: 2,
  },

  topSection: { height: BANNER_H, position: "relative", backgroundColor: "#e9e9ea" },
  banner: {
    width: "100%",
    height: "100%",
    // top radii not required because parent container now handles all corners.
    // Keeping them here is harmless, but we remove them for consistency:
  },
  bannerSkeleton: {
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "#e9e9ea",
  },
  bannerSkeletonText: { 
    color: "#9ca3af", 
    fontSize: 16, 
    fontWeight: "500",
    opacity: 0.7,
  },
  bannerPlaceholder: {
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "transparent",
  },
  bannerPlaceholderText: { color: "#6b7280", fontSize: 16, fontWeight: "600" },

  contentArea: {
    flex: 1,
    paddingHorizontal: 16,
    paddingTop: 4,
    minHeight: SCREEN_H * 0.6, // Minimum height to prevent jumps
  },

  headerRow: { flexDirection: "row", alignItems: "center", marginBottom: 2 },
  headerLeft: { flex: 1 },
  headerRight: { marginLeft: 8, alignItems: "flex-end" },

  companyHeaderText: { fontSize: 16, fontWeight: "700", color: "#111827" },
  companySubText: { fontSize: 13, color: "#6b7280", marginTop: 2 },

  dateSentimentRow: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: 4,
  },

  announcementDateText: { fontSize: 12, color: "#6b7280" },

  smallLogo: { width: 36, height: 36, borderRadius: 6 },
  logoPlaceholder: {
    width: 36,
    height: 36,
    borderRadius: 6,
    backgroundColor: "#eef2ff",
    alignItems: "center",
    justifyContent: "center",
  },
  logoPlaceholderText: { fontSize: 14, fontWeight: "700", color: "#1E40AF" },

  summary: {
    fontSize: 15,
    color: "#111827",
    marginBottom: 8,
    fontWeight: "400",
    textAlign: "justify",
  },
  body: {
    fontSize: 14,
    color: "#374151",
    marginBottom: 12,
    textAlign: "justify",
  },

  snapshotCard: {
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#e6e6e6",
    padding: 12,
    marginBottom: 12,
    backgroundColor: "#f9fafb",
  },
  snapshotHeader: {
    fontSize: 12,
    color: "#6b7280",
    marginBottom: 8,
    fontWeight: "600",
  },

  gridRow: { flexDirection: "row", justifyContent: "space-between" },
  gridCol: { flex: 1, paddingHorizontal: 6, alignItems: "flex-start" },
  gridLabel: { color: "#6b7280", fontSize: 13 },
  gridValue: { fontSize: 13, fontWeight: "700", color: "#111827" },

  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingVertical: 6,
  },
  label: { color: "#6b7280", fontSize: 13 },
  value: { fontSize: 13, fontWeight: "600", color: "#111827" },

  snapshotEmpty: { fontSize: 12, color: "#9ca3af", textAlign: "center" },

  tradeBtn: {
    backgroundColor: "#1E40AF",
    paddingVertical: 12,
    borderRadius: 8,
    marginTop: 12,
    alignItems: "center",
  },
  tradeText: { color: "#fff", fontWeight: "600" },

  badge: { paddingHorizontal: 8, paddingVertical: 4, borderRadius: 8 },
  badgeText: { color: "#000", fontSize: 12, fontWeight: "500" },

  positive: { color: "#16a34a" },
  negative: { color: "#dc2626" },
});
