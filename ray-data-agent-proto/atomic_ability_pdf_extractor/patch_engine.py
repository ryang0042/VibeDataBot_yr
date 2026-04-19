import json
import re
import uuid
from collections import defaultdict
from statistics import median

import fitz
from shapely.geometry import Point, box
from shapely.ops import unary_union


class PatchEngine:
    def __init__(self, pdf_path, raw_json_path):
        self.pdf_path = pdf_path
        with open(raw_json_path, "r", encoding="utf-8") as f:
            self.items = json.load(f)

        # 保存原始 Docling 结果，供后续“有效文本区域”硬约束使用。
        self.raw_items = json.loads(json.dumps(self.items))
        for item in self.items:
            item.setdefault("text", "")
            item.setdefault("text_preview", "")
            item.setdefault("ancestor_labels", [])
            item.setdefault("ancestor_container_label", None)
            item["_uid"] = str(uuid.uuid4())
            item["_origin_id"] = item.get("id", -1)

        self.doc = fitz.open(pdf_path)
        self.page_raw_envelope = self._build_raw_envelopes()
        self.page_noise_zones = defaultdict(list)

    def _build_raw_envelopes(self):
        env = {}
        by_page = defaultdict(list)
        for it in self.raw_items:
            by_page[it["page"]].append(it["bbox"])
        for page_no, bboxes in by_page.items():
            if not bboxes:
                continue
            x0 = min(b[0] for b in bboxes)
            y0 = min(b[1] for b in bboxes)
            x1 = max(b[2] for b in bboxes)
            y1 = max(b[3] for b in bboxes)
            env[page_no] = [x0, y0, x1, y1]
        return env

    @staticmethod
    def _bbox_area(coords):
        return max(0.0, coords[2] - coords[0]) * max(0.0, coords[3] - coords[1])

    @staticmethod
    def _bbox_iou(a, b):
        ax0, ay0, ax1, ay1 = a
        bx0, by0, bx1, by1 = b
        ix0 = max(ax0, bx0)
        iy0 = max(ay0, by0)
        ix1 = min(ax1, bx1)
        iy1 = min(ay1, by1)
        iw = max(0.0, ix1 - ix0)
        ih = max(0.0, iy1 - iy0)
        inter = iw * ih
        if inter <= 0:
            return 0.0
        a_area = PatchEngine._bbox_area(a)
        b_area = PatchEngine._bbox_area(b)
        union = a_area + b_area - inter
        return inter / union if union > 0 else 0.0

    @staticmethod
    def _bbox_intersection_area(a, b):
        ax0, ay0, ax1, ay1 = a
        bx0, by0, bx1, by1 = b
        ix0 = max(ax0, bx0)
        iy0 = max(ay0, by0)
        ix1 = min(ax1, bx1)
        iy1 = min(ay1, by1)
        return max(0.0, ix1 - ix0) * max(0.0, iy1 - iy0)

    @staticmethod
    def _bbox_center(coords):
        x0, y0, x1, y1 = coords
        return (x0 + x1) / 2.0, (y0 + y1) / 2.0

    @staticmethod
    def _normalize_text(text):
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _item_text(item):
        text = item.get("text") or item.get("text_preview") or ""
        return text.strip()

    @staticmethod
    def _normalize_signature(text):
        text = PatchEngine._normalize_text(text)
        if not text:
            return ""
        text = re.sub(r"\d+", "", text)
        text = re.sub(r"[^a-z\u4e00-\u9fff]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _is_margin_position(bbox, page_rect):
        x0, y0, x1, y1 = bbox
        page_w = page_rect.width
        page_h = page_rect.height
        near_top = y0 < page_h * 0.1
        near_bottom = y1 > page_h * 0.9
        near_left = x0 < page_w * 0.05
        near_right = x1 > page_w * 0.95
        return near_top or near_bottom or near_left or near_right

    @staticmethod
    def _looks_like_structured_section_header(text):
        """
        常见真实章节标题模式：
        1. Introduction / 2.3 Methods / II. METHOD / Appendix A
        """
        t = PatchEngine._normalize_text(text)
        if not t:
            return False
        patterns = [
            r"^\d+(\.\d+)*\s",
            r"^[ivxlcdm]+\.\s",
            r"^appendix\s+[a-z0-9]+",
            r"^chapter\s+\d+",
            r"^section\s+\d+",
        ]
        return any(re.match(p, t) for p in patterns)

    def _looks_like_margin_noise(self, item, page_rect):
        text = self._normalize_text(self._item_text(item))
        x0, y0, x1, y1 = item["bbox"]
        w = x1 - x0
        h = y1 - y0
        page_w = page_rect.width
        page_h = page_rect.height

        near_top = y0 < page_h * 0.08
        near_bottom = y1 > page_h * 0.94
        near_side = x0 < page_w * 0.03 or x1 > page_w * 0.97

        # Typical side watermark pattern: very narrow and very tall.
        if near_side and w < page_w * 0.09 and h > page_h * 0.25:
            return True

        noisy_keywords = (
            "arxiv",
            "preprint",
            "copyright",
            "all rights reserved",
            "replace this line",
            "mnras",
        )
        if text and (near_top or near_bottom or near_side):
            if any(keyword in text for keyword in noisy_keywords):
                return True

        # page number / short marker near top or bottom
        if near_top or near_bottom:
            compact = re.sub(r"[^a-z0-9]", "", text)
            if compact and len(compact) <= 4:
                return True

        return False

    def _build_body_envelope(self, page_items, page_rect):
        """
        从当前页 Docling 已有框中提取“主体区域包络”，
        用于限制补漏不越界到页眉/页脚/侧边水印区域。
        """
        body_like_labels = {
            "TEXT",
            "LIST_ITEM",
            "SECTION_HEADER",
            "TITLE",
            "FOOTNOTE",
            "FORMULA",
            "CAPTION",
            "COMPLEX_BLOCK",
        }
        core = []
        for it in page_items:
            if it["label"] not in body_like_labels:
                continue
            x0, y0, x1, y1 = it["bbox"]
            w = x1 - x0
            h = y1 - y0
            if w <= 0 or h <= 0:
                continue
            if self._looks_like_margin_noise(it, page_rect):
                continue
            # 去掉侧边窄高条，避免污染主体 x/y 包络。
            if w < page_rect.width * 0.1 and h > page_rect.height * 0.22:
                continue
            core.append((x0, y0, x1, y1))

        if len(core) < 4:
            return None

        xs0 = [c[0] for c in core]
        ys0 = [c[1] for c in core]
        xs1 = [c[2] for c in core]
        ys1 = [c[3] for c in core]

        # 使用分位数抑制页眉/页脚极值点对包络的污染。
        bx0 = self._percentile(xs0, 0.05)
        bx1 = self._percentile(xs1, 0.95)
        by0 = self._percentile(ys0, 0.12)
        by1 = self._percentile(ys1, 0.95)
        if bx0 is None or bx1 is None or by0 is None or by1 is None:
            return None

        if bx1 <= bx0 or by1 <= by0:
            return None
        return [bx0, by0, bx1, by1]

    @staticmethod
    def _equation_signal_score(text):
        if not text:
            return 0
        score = 0
        if re.search(r"[=<>±×÷∑∏∫√∞∂∇≈≠≤≥→←↔]", text):
            score += 2
        sym_cnt = len(re.findall(r"[\=\+\-\*/\^_<>±×÷∑∏∫√∞∂∇≈≠≤≥\(\)\[\]\{\}|]", text))
        alnum_cnt = len(re.findall(r"[A-Za-z0-9]", text))
        if sym_cnt >= max(4, int(alnum_cnt * 0.28)):
            score += 1
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if len(lines) >= 2 and sum(1 for ln in lines if len(ln) <= 24) >= 2:
            score += 1
        if re.search(r"\(\s*\d+\s*\)\s*$", text.replace("\n", " ").strip()):
            score += 1
        return score

    @staticmethod
    def _percentile(values, q):
        if not values:
            return None
        arr = sorted(values)
        if len(arr) == 1:
            return arr[0]
        idx = int(round((len(arr) - 1) * q))
        idx = max(0, min(len(arr) - 1, idx))
        return arr[idx]

    @staticmethod
    def _looks_like_equation_number(text):
        if not text:
            return False
        t = text.strip()
        return bool(re.match(r"^[\(\[\{]?\s*\d+([\-\.]\d+)?\s*[\)\]\}]?$", t))

    @staticmethod
    def _center_in_bbox(center, bbox):
        cx, cy = center
        return bbox[0] <= cx <= bbox[2] and bbox[1] <= cy <= bbox[3]

    @staticmethod
    def _bbox_union(bboxes):
        return [
            min(b[0] for b in bboxes),
            min(b[1] for b in bboxes),
            max(b[2] for b in bboxes),
            max(b[3] for b in bboxes),
        ]

    def _bbox_overlap_ratio(self, a, b):
        inter = self._bbox_intersection_area(a, b)
        if inter <= 0:
            return 0.0
        return inter / max(1.0, self._bbox_area(a))

    @staticmethod
    def _bbox_overlap_x_ratio(a, b):
        a_w = max(1.0, a[2] - a[0])
        b_w = max(1.0, b[2] - b[0])
        overlap_x = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
        return overlap_x / min(a_w, b_w)

    @staticmethod
    def _is_formula_like_complex(item):
        if item.get("label") != "COMPLEX_BLOCK":
            return False
        tag = (item.get("text") or item.get("text_preview") or "").upper()
        return (
            "FORMULA" in tag
            or "NUMBER_ANCHOR" in tag
            or "OVERLAP_COMPLEX_REGION" in tag
            or "FORMULA_STACK" in tag
        )

    def _anchor_search_window(self, anchor_bbox, page_w, page_h, word_line_h, target_profile, col_mid_x):
        fx0, fy0, fx1, fy1 = anchor_bbox
        if target_profile:
            x_pad = max(page_w * 0.035, word_line_h * 2.0)
            sx0 = max(0.0, target_profile["x0"] - x_pad)
            sx1 = min(page_w, target_profile["x1"] + x_pad)
        else:
            anchor_cx = (fx0 + fx1) / 2.0
            in_left_col = anchor_cx < col_mid_x
            sx0 = max(0.0, fx0 - page_w * 0.34 if in_left_col else col_mid_x - page_w * 0.02)
            sx1 = min(page_w, col_mid_x + page_w * 0.02 if in_left_col else fx1 + page_w * 0.34)
        sy0 = max(0.0, fy0 - max(word_line_h * 3.6, 34.0))
        sy1 = min(page_h, fy1 + max(word_line_h * 4.5, 42.0))
        return [sx0, sy0, sx1, sy1]

    def _collect_anchor_text_hints(self, page, anchor_bbox, anchor_text, search, word_line_h):
        anchor_text_norm = self._normalize_text(anchor_text)
        fx0, fy0, fx1, fy1 = anchor_bbox
        hints = []
        seen = set()
        for block in page.get_text("blocks"):
            if len(block) < 7 or block[6] != 0:
                continue
            bb = [float(block[0]), float(block[1]), float(block[2]), float(block[3])]
            b_area = self._bbox_area(bb)
            if b_area <= 0:
                continue
            if box(*bb).intersection(box(*search)).area / b_area < 0.3:
                continue
            block_text = self._normalize_text((block[4] or "").replace("\n", " "))
            if anchor_text_norm:
                if anchor_text_norm != block_text:
                    continue
            else:
                if not self._looks_like_equation_number(block_text):
                    continue
            vertical_gap = max(0.0, max(fy0, bb[1]) - min(fy1, bb[3]))
            if vertical_gap > max(word_line_h * 0.9, 8.0):
                continue
            if (bb[2] - bb[0]) <= (fx1 - fx0) + word_line_h * 1.6:
                continue
            key = tuple(round(v, 2) for v in bb)
            if key in seen:
                continue
            seen.add(key)
            hints.append(bb)
        return hints

    def _collect_anchor_band_drawings(self, page, search, anchor_bbox, target_profile, page_w, word_line_h):
        fx0, fy0, fx1, fy1 = anchor_bbox
        band = [
            max(search[0], 0.0),
            max(0.0, fy0 - max(word_line_h * 0.95, 8.0)),
            min(search[2], page_w),
            fy1 + max(word_line_h * 0.95, 8.0),
        ]
        band_geom = box(*band)
        raw_boxes = []
        for d in page.get_drawings() or []:
            rect = d.get("rect")
            if not rect:
                continue
            db = [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)]
            d_area = self._bbox_area(db)
            if d_area < 4.0:
                continue
            if band_geom.intersection(box(*db)).area / max(1.0, d_area) < 0.25:
                continue
            db_cx = (db[0] + db[2]) / 2.0
            if target_profile:
                profile_pad = max(page_w * 0.02, word_line_h * 1.5)
                if db_cx < target_profile["x0"] - profile_pad or db_cx > target_profile["x1"] + profile_pad:
                    continue
            raw_boxes.append(db)

        if not raw_boxes:
            return []

        clusters = self._cluster_boxes(
            raw_boxes,
            x_pad=max(word_line_h * 0.42, 1.4),
            y_pad=max(word_line_h * 0.3, 0.9),
        )
        if not clusters:
            return []

        seed_gap = max(word_line_h * 4.0, 40.0)
        chain_gap = max(word_line_h * 3.0, 28.0)
        anchor_span = [fx0, fx1]
        selected = []
        selected_members = []

        def h_gap(a, b):
            if a[1] < b[0]:
                return b[0] - a[1]
            if b[1] < a[0]:
                return a[0] - b[1]
            return 0.0

        chosen = set()
        changed = True
        while changed:
            changed = False
            for idx, cluster in enumerate(clusters):
                if idx in chosen:
                    continue
                cb = cluster["bbox"]
                c_span = [cb[0], cb[2]]
                vertical_gap = max(0.0, max(fy0, cb[1]) - min(fy1, cb[3]))
                if vertical_gap > max(word_line_h * 1.0, 10.0):
                    continue
                if h_gap(c_span, anchor_span) <= seed_gap:
                    chosen.add(idx)
                    selected.append(cluster)
                    selected_members.extend(cluster["members"])
                    changed = True
                    continue
                if any(h_gap(c_span, [sc["bbox"][0], sc["bbox"][2]]) <= chain_gap for sc in selected):
                    chosen.add(idx)
                    selected.append(cluster)
                    selected_members.extend(cluster["members"])
                    changed = True

        if not selected_members:
            return []

        merged_bbox = self._bbox_union(selected_members)
        if (merged_bbox[2] - merged_bbox[0]) < max(word_line_h * 6.5, 56.0):
            return []
        return selected_members

    def _formula_stack_text_bridgeable(self, text_node, nodes, page_w, word_line_h):
        if text_node["label"] not in {"TEXT", "LIST_ITEM"}:
            return False

        t_text = self._item_text(text_node)
        t_box = text_node["bbox"]
        t_height = t_box[3] - t_box[1]
        if t_height > max(word_line_h * 5.0, 56.0):
            return False

        t_width = t_box[2] - t_box[0]
        text_len = len(self._normalize_text(t_text))
        if t_width > max(page_w * 0.42, word_line_h * 24.0):
            return False
        if text_len > 120:
            return False

        has_formula_above = False
        has_formula_below = False
        for probe in nodes:
            if probe["_uid"] == text_node["_uid"]:
                continue
            if probe["label"] not in {"FORMULA", "COMPLEX_BLOCK"}:
                continue
            p_box = probe["bbox"]
            p_overlap_x = max(
                0.0, min(t_box[2], p_box[2]) - max(t_box[0], p_box[0])
            ) / max(1.0, min(t_box[2] - t_box[0], p_box[2] - p_box[0]))
            if p_overlap_x < 0.55:
                continue
            above_gap = t_box[1] - p_box[3]
            below_gap = p_box[1] - t_box[3]
            if 0 <= above_gap <= max(word_line_h * 1.8, 20.0):
                has_formula_above = True
            if 0 <= below_gap <= max(word_line_h * 1.8, 20.0):
                has_formula_below = True
            if has_formula_above and has_formula_below:
                break

        if self._looks_like_equation_number(t_text):
            return True
        if bool(re.search(r"\(\s*\d+[a-z]?\s*\)", t_text.lower())):
            return True
        if self._equation_signal_score(t_text) >= 1:
            return True
        return has_formula_above and has_formula_below

    @staticmethod
    def _formula_complex_text_overlap_significant(overlap_min, overlap_by_text, overlap_x, text_height, word_line_h):
        if overlap_by_text >= 0.42:
            return True
        if overlap_min >= 0.16:
            return True
        return (
            overlap_by_text >= 0.14
            and text_height <= max(word_line_h * 2.8, 34.0)
            and overlap_x >= 0.55
        )

    @staticmethod
    def _is_formula_complex_edge_strip_overlap(complex_bbox, text_bbox, word_line_h):
        text_height = max(1.0, text_bbox[3] - text_bbox[1])
        if text_height <= max(word_line_h * 4.0, 44.0):
            return False

        inter_y = max(0.0, min(complex_bbox[3], text_bbox[3]) - max(complex_bbox[1], text_bbox[1]))
        if inter_y <= 1.0:
            return False
        overlap_by_text_y = inter_y / text_height
        if overlap_by_text_y < 0.08 or overlap_by_text_y > 0.38:
            return False

        overlap_x = max(0.0, min(complex_bbox[2], text_bbox[2]) - max(complex_bbox[0], text_bbox[0]))
        min_width = max(1.0, min(complex_bbox[2] - complex_bbox[0], text_bbox[2] - text_bbox[0]))
        if overlap_x / min_width < 0.72:
            return False

        edge_tol = max(word_line_h * 2.2, 24.0)
        near_text_bottom = abs(complex_bbox[1] - text_bbox[3]) <= edge_tol
        near_text_top = abs(complex_bbox[3] - text_bbox[1]) <= edge_tol
        return near_text_bottom or near_text_top

    def _should_absorb_into_formula_complex(self, complex_bbox, other, page_w, word_line_h):
        inter = self._bbox_intersection_area(complex_bbox, other["bbox"])
        if inter <= 6.0:
            return False

        c_area = max(1.0, self._bbox_area(complex_bbox))
        o_area = max(1.0, self._bbox_area(other["bbox"]))
        overlap_min = inter / min(c_area, o_area)
        overlap_by_other = inter / o_area
        overlap_x = self._bbox_overlap_x_ratio(complex_bbox, other["bbox"])
        center_other_in_complex = self._center_in_bbox(self._bbox_center(other["bbox"]), complex_bbox)

        if other["label"] in {"PICTURE", "TABLE", "FORMULA", "COMPLEX_BLOCK"}:
            return (
                center_other_in_complex
                or overlap_by_other >= 0.18
                or overlap_min >= 0.12
            )

        if other["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER"}:
            text_height = other["bbox"][3] - other["bbox"][1]
            return center_other_in_complex or self._formula_complex_text_overlap_significant(
                overlap_min,
                overlap_by_other,
                overlap_x,
                text_height,
                word_line_h,
            )

        return False

    @staticmethod
    def _cluster_text_lines(lines, max_vgap, x_tolerance):
        if not lines:
            return []
        lines = sorted(lines, key=lambda ln: (ln["bbox"][1], ln["bbox"][0]))
        clusters = [[lines[0]]]
        for line in lines[1:]:
            prev = clusters[-1][-1]
            prev_bbox = prev["bbox"]
            cur_bbox = line["bbox"]
            vertical_gap = cur_bbox[1] - prev_bbox[3]
            left_shift = abs(cur_bbox[0] - prev_bbox[0])
            right_shift = abs(cur_bbox[2] - prev_bbox[2])
            overlap_x = max(0.0, min(cur_bbox[2], prev_bbox[2]) - max(cur_bbox[0], prev_bbox[0]))
            min_width = max(1.0, min(cur_bbox[2] - cur_bbox[0], prev_bbox[2] - prev_bbox[0]))
            same_band = overlap_x / min_width >= 0.22 or (left_shift <= x_tolerance and right_shift <= x_tolerance * 1.8)
            if vertical_gap <= max_vgap and same_band:
                clusters[-1].append(line)
            else:
                clusters.append([line])
        return clusters

    def _overlaps_noise_zone(self, bbox, page_no, threshold=0.2):
        center = self._bbox_center(bbox)
        for zone in self.page_noise_zones.get(page_no, []):
            if self._bbox_overlap_ratio(bbox, zone) >= threshold:
                return True
            if self._center_in_bbox(center, zone):
                return True
        return False

    def _looks_like_running_header_candidate(self, text, bbox, page_rect, page_no, typical_text_h):
        text = self._normalize_text(text)
        if not text:
            return False

        x0, y0, x1, y1 = bbox
        w = x1 - x0
        h = y1 - y0
        near_top = y0 < page_rect.height * 0.12
        near_bottom = y1 > page_rect.height * 0.9
        line_ref = min(typical_text_h, 16.0)
        single_line = h <= max(line_ref * 1.7, 18.0)
        no_terminal_punct = not bool(re.search(r"[.!?;:]$", text))

        if near_top and re.match(r"^\d+\s+[a-z][a-z\.\-'\s]+$", text):
            return True
        if near_top and page_no > 1 and single_line and w > page_rect.width * 0.34:
            if (
                no_terminal_punct
                and not self._looks_like_structured_section_header(text)
                and "abstract" not in text
            ):
                return True
        if near_bottom and single_line and w > page_rect.width * 0.28:
            return True
        return False

    def _compute_column_profiles(self, page_items, page_rect):
        text_like = [
            it
            for it in page_items
            if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "FOOTNOTE"}
            and not self._looks_like_margin_noise(it, page_rect)
        ]
        if len(text_like) < 2:
            return []

        centers = [self._bbox_center(it["bbox"])[0] for it in text_like]
        spread = max(centers) - min(centers)
        if len(text_like) < 5 or spread < page_rect.width * 0.28:
            groups = [text_like]
        else:
            mid_x = (min(centers) + max(centers)) / 2.0
            left = [it for it in text_like if self._bbox_center(it["bbox"])[0] < mid_x]
            right = [it for it in text_like if self._bbox_center(it["bbox"])[0] >= mid_x]
            groups = [grp for grp in (left, right) if grp]

        profiles = []
        for grp in groups:
            xs0 = sorted(it["bbox"][0] for it in grp)
            xs1 = sorted(it["bbox"][2] for it in grp)
            widths = sorted((it["bbox"][2] - it["bbox"][0]) for it in grp)
            ys0 = sorted(it["bbox"][1] for it in grp)
            if not xs0 or not xs1:
                continue
            profiles.append(
                {
                    "items": grp,
                    "x0": self._percentile(xs0, 0.15),
                    "x1": self._percentile(xs1, 0.85),
                    "y0": self._percentile(ys0, 0.1),
                    "median_width": self._percentile(widths, 0.5),
                }
            )
        return profiles

    def _profile_idx_for_bbox(self, bbox, column_profiles, page_w, word_line_h):
        if not column_profiles:
            return None
        if len(column_profiles) == 1:
            return 0

        cx = (bbox[0] + bbox[2]) / 2.0
        x_pad = max(page_w * 0.03, word_line_h * 2.0)
        candidates = []
        for idx, profile in enumerate(column_profiles):
            profile_center = (profile["x0"] + profile["x1"]) / 2.0
            overlap_x = self._bbox_overlap_x_ratio(
                bbox,
                [profile["x0"], bbox[1], profile["x1"], bbox[3]],
            )
            in_band = profile["x0"] - x_pad <= cx <= profile["x1"] + x_pad
            if overlap_x >= 0.2 or in_band:
                candidates.append((overlap_x, -abs(profile_center - cx), idx))

        if candidates:
            candidates.sort(reverse=True)
            return candidates[0][2]

        return min(
            range(len(column_profiles)),
            key=lambda idx: abs(((column_profiles[idx]["x0"] + column_profiles[idx]["x1"]) / 2.0) - cx),
        )

    def _component_spans_multiple_profiles(self, bboxes, column_profiles, page_w, word_line_h):
        if len(column_profiles) < 2:
            return False
        profile_ids = {
            self._profile_idx_for_bbox(bbox, column_profiles, page_w, word_line_h)
            for bbox in bboxes
        }
        profile_ids.discard(None)
        return len(profile_ids) > 1

    @staticmethod
    def _group_consecutive_indices(indices):
        if not indices:
            return []
        groups = [[indices[0]]]
        for idx in indices[1:]:
            if idx == groups[-1][-1] + 1:
                groups[-1].append(idx)
            else:
                groups.append([idx])
        return groups

    def _looks_like_folio_token(self, text, bbox, page_rect, word_line_h):
        text = self._normalize_text(text)
        if not text:
            return False
        compact = re.sub(r"[^a-z0-9]", "", text)
        if not compact or len(compact) > 4:
            return False
        x0, y0, x1, y1 = bbox
        w = x1 - x0
        h = y1 - y0
        if h > max(word_line_h * 1.6, 18.0):
            return False
        if w > max(page_rect.width * 0.08, word_line_h * 2.6):
            return False
        return y0 < page_rect.height * 0.14 or y1 > page_rect.height * 0.9

    @staticmethod
    def _extract_text_lines(page):
        lines = []
        text_dict = page.get_text("dict")
        for block in text_dict.get("blocks", []):
            if block.get("type") != 0:
                continue
            for line in block.get("lines", []):
                spans = line.get("spans", [])
                line_text = "".join(span.get("text", "") for span in spans).strip().replace("\n", " ")
                if not line_text:
                    continue
                bbox = [float(v) for v in line.get("bbox", [0, 0, 0, 0])]
                if bbox[2] <= bbox[0] or bbox[3] <= bbox[1]:
                    continue
                lines.append({"bbox": bbox, "text": line_text})
        return lines

    @staticmethod
    def _extract_text_blocks_with_lines(page):
        blocks = []
        text_dict = page.get_text("dict")
        for block in text_dict.get("blocks", []):
            if block.get("type") != 0:
                continue
            block_lines = []
            text_parts = []
            for line in block.get("lines", []):
                spans = line.get("spans", [])
                line_text = "".join(span.get("text", "") for span in spans).strip().replace("\n", " ")
                if not line_text:
                    continue
                bbox = [float(v) for v in line.get("bbox", [0, 0, 0, 0])]
                if bbox[2] <= bbox[0] or bbox[3] <= bbox[1]:
                    continue
                block_lines.append({"bbox": bbox, "text": line_text})
                text_parts.append(line_text)
            if not block_lines:
                continue
            block_bbox = [
                min(ln["bbox"][0] for ln in block_lines),
                min(ln["bbox"][1] for ln in block_lines),
                max(ln["bbox"][2] for ln in block_lines),
                max(ln["bbox"][3] for ln in block_lines),
            ]
            blocks.append(
                {
                    "bbox": block_bbox,
                    "text": " ".join(text_parts).strip(),
                    "lines": block_lines,
                }
            )
        return blocks

    @staticmethod
    def _estimate_word_line_height(page):
        heights = []
        for word in page.get_text("words"):
            if len(word) < 4:
                continue
            h = float(word[3]) - float(word[1])
            if 6.0 <= h <= 40.0:
                heights.append(h)
        if not heights:
            return 11.0
        return median(heights)

    @staticmethod
    def _cluster_boxes(bboxes, x_pad, y_pad):
        if not bboxes:
            return []
        expanded = [
            [b[0] - x_pad, b[1] - y_pad, b[2] + x_pad, b[3] + y_pad]
            for b in bboxes
        ]
        edges = []
        for i in range(len(bboxes)):
            for j in range(i + 1, len(bboxes)):
                if PatchEngine._bbox_intersection_area(expanded[i], expanded[j]) > 0:
                    edges.append((i, j))

        if not edges:
            return [{"bbox": bboxes[0], "members": [bboxes[0]]}] if len(bboxes) == 1 else [
                {"bbox": b, "members": [b]} for b in bboxes
            ]

        clusters = []
        for comp in PatchEngine._components_from_edges(len(bboxes), edges):
            members = [bboxes[idx] for idx in comp]
            clusters.append(
                {
                    "bbox": [
                        min(b[0] for b in members),
                        min(b[1] for b in members),
                        max(b[2] for b in members),
                        max(b[3] for b in members),
                    ],
                    "members": members,
                }
            )
        return clusters

    def workshop_1_scavenger(self):
        """阶段1-A：删除 table/toc/document_index/picture 内的幽灵文本"""
        to_remove_uids = set()
        pages = sorted(set(it["page"] for it in self.items))
        removed_by_reason = defaultdict(int)

        for page_no in pages:
            page_items = [it for it in self.items if it["page"] == page_no]
            containers = [
                it
                for it in page_items
                if it["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC", "PICTURE"}
            ]
            candidates = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"}]
            if not containers or not candidates:
                continue

            page_rect = self.doc[page_no - 1].rect
            page_area = page_rect.width * page_rect.height

            for cand in candidates:
                cand_box = box(*cand["bbox"])
                cand_area = cand_box.area
                if cand_area <= 0:
                    continue
                container_hint = cand.get("ancestor_container_label")
                if container_hint in {"TABLE", "DOCUMENT_INDEX", "TOC"}:
                    to_remove_uids.add(cand["_uid"])
                    removed_by_reason[f"{container_hint}_STRUCT"] += 1
                    continue

                center_x, center_y = self._bbox_center(cand["bbox"])
                cand_text = self._item_text(cand)
                text_len = len(self._normalize_text(cand_text))

                if container_hint == "PICTURE":
                    small_picture_text = text_len <= 120 and cand_area < page_area * 0.08
                    if small_picture_text:
                        to_remove_uids.add(cand["_uid"])
                        removed_by_reason["PICTURE_STRUCT"] += 1
                        continue

                for cont in containers:
                    cont_box = box(*cont["bbox"])
                    cont_area = cont_box.area
                    if cont_area <= 0:
                        continue
                    if cont_area > page_area * 0.9:
                        continue

                    inclusion_ratio = cand_box.intersection(cont_box).area / cand_area
                    center_inside = cont_box.covers(Point(center_x, center_y))

                    if cont["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC"} and inclusion_ratio >= 0.93:
                        to_remove_uids.add(cand["_uid"])
                        removed_by_reason[cont["label"]] += 1
                        break

                    # 边界场景：文本框并非真子集，但中心点已经在容器内，且大部分重叠。
                    if (
                        cont["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC"}
                        and inclusion_ratio >= 0.6
                        and center_inside
                        and text_len <= 160
                    ):
                        to_remove_uids.add(cand["_uid"])
                        removed_by_reason[f"{cont['label']}_CENTER"] += 1
                        break

                    # PICTURE 内文本只做强条件删除，避免误杀图片内部真实文字说明块。
                    if (
                        cont["label"] == "PICTURE"
                        and inclusion_ratio >= 0.98
                        and cand_area < cont_area * 0.2
                        and text_len <= 80
                    ):
                        to_remove_uids.add(cand["_uid"])
                        removed_by_reason["PICTURE"] += 1
                        break

        self.items = [it for it in self.items if it["_uid"] not in to_remove_uids]
        detail = ", ".join(f"{k}:{v}" for k, v in sorted(removed_by_reason.items())) or "无"
        print(f"    [Workshop 1] 幽灵文本清理完成，共删除 {len(to_remove_uids)} 个。明细: {detail}")

    def workshop_2_margin_noise_filter(self):
        """阶段1-B：删除页眉/页脚/侧边水印类噪声（优先跨页重复 + 靠边规则）"""
        removable = set()
        removed_by_label = defaultdict(int)

        def mark_remove(item):
            uid = item["_uid"]
            if uid in removable:
                return
            removable.add(uid)
            removed_by_label[item["label"]] += 1

        text_like = {
            "TEXT",
            "LIST_ITEM",
            "SECTION_HEADER",
            "TITLE",
            "FOOTNOTE",
            "PAGE_HEADER",
            "PAGE_FOOTER",
            "CAPTION",
        }
        sig_to_items = defaultdict(list)

        for it in self.items:
            if it["label"] not in text_like:
                continue
            page_rect = self.doc[it["page"] - 1].rect
            if not self._is_margin_position(it["bbox"], page_rect):
                continue
            sig = self._normalize_signature(self._item_text(it))
            if len(sig) < 8:
                continue
            sig_to_items[sig].append(it)

        # 跨页重复 + 靠边：高概率是 running header/footer
        for sig, items in sig_to_items.items():
            page_set = {it["page"] for it in items}
            if len(page_set) >= 3:
                for it in items:
                    mark_remove(it)

        # 单页规则：侧边高条水印 / 顶底噪声关键字
        for it in self.items:
            if it["label"] not in text_like:
                continue
            page_rect = self.doc[it["page"] - 1].rect
            if self._looks_like_margin_noise(it, page_rect):
                mark_remove(it)

        # 标签无关规则：侧边窄高条直接剔除（常见 arXiv 侧边水印）。
        for it in self.items:
            if it["label"] in {"TABLE", "PICTURE"}:
                continue
            page_rect = self.doc[it["page"] - 1].rect
            x0, y0, x1, y1 = it["bbox"]
            w = x1 - x0
            h = y1 - y0
            near_side = x0 < page_rect.width * 0.035 or x1 > page_rect.width * 0.965
            narrow_tall = w < page_rect.width * 0.11 and h > page_rect.height * 0.24
            if near_side and narrow_tall:
                mark_remove(it)

        # Docling 有时会把 running header 标成 SECTION_HEADER / TITLE。
        for it in self.items:
            if it["label"] not in {"SECTION_HEADER", "TITLE"}:
                continue
            page_rect = self.doc[it["page"] - 1].rect
            x0, y0, x1, y1 = it["bbox"]
            w = x1 - x0
            h = y1 - y0
            near_top = y0 < page_rect.height * 0.09
            near_bottom = y1 > page_rect.height * 0.93
            wide_short = w > page_rect.width * 0.48 and h < page_rect.height * 0.035
            if not (near_top or near_bottom):
                continue
            if not wide_short:
                continue
            if it["page"] == 1:
                # 首页顶部宽标题通常是真标题，优先保留。
                continue
            if self._looks_like_structured_section_header(self._item_text(it)):
                continue
            mark_remove(it)

        for it in self.items:
            if it["_uid"] not in removable:
                continue
            self.page_noise_zones[it["page"]].append(it["bbox"])

        self.items = [it for it in self.items if it["_uid"] not in removable]
        detail = ", ".join(f"{k}:{v}" for k, v in sorted(removed_by_label.items())) or "无"
        print(f"    [Workshop 2] 页边噪声过滤完成，删除 {len(removable)} 个疑似页眉/页脚/水印文本块。明细: {detail}")

    def workshop_3_formula_text_resolver(self):
        """阶段2-A：处理 TEXT/LIST_ITEM 与 FORMULA 大面积重叠冲突"""
        pages = sorted(set(it["page"] for it in self.items))
        remove_uids = set()
        relabel_count = 0
        remove_count = 0

        for page_no in pages:
            page_items = [it for it in self.items if it["page"] == page_no]
            formulas = [it for it in page_items if it["label"] == "FORMULA"]
            text_candidates = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"}]
            if not formulas or not text_candidates:
                continue

            formula_boxes = [box(*it["bbox"]) for it in formulas]

            for cand in text_candidates:
                cand_box = box(*cand["bbox"])
                cand_area = cand_box.area
                if cand_area <= 0:
                    continue

                overlap_area = 0.0
                for f_box in formula_boxes:
                    overlap_area += cand_box.intersection(f_box).area
                overlap_ratio = overlap_area / cand_area
                text_len = len(self._normalize_text(self._item_text(cand)))

                # 小碎片并且被公式高度覆盖，直接删除。
                if overlap_ratio >= 0.7 and cand_area < 900:
                    remove_uids.add(cand["_uid"])
                    remove_count += 1
                    continue

                # 大面积重叠时，不做语义硬判，直接整体降级为复杂块交给后续图像链路。
                if overlap_ratio >= 0.42 or (overlap_ratio >= 0.28 and text_len <= 18):
                    cand["label"] = "COMPLEX_BLOCK"
                    relabel_count += 1

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        print(
            f"    [Workshop 3] 公式冲突处理完成：删除碎片 {remove_count} 个，降级 COMPLEX_BLOCK {relabel_count} 个。"
        )

    @staticmethod
    def _components_from_edges(node_count, edges):
        graph = [[] for _ in range(node_count)]
        for a, b in edges:
            graph[a].append(b)
            graph[b].append(a)
        seen = [False] * node_count
        comps = []
        for i in range(node_count):
            if seen[i]:
                continue
            stack = [i]
            seen[i] = True
            comp = []
            while stack:
                cur = stack.pop()
                comp.append(cur)
                for nxt in graph[cur]:
                    if not seen[nxt]:
                        seen[nxt] = True
                        stack.append(nxt)
            comps.append(comp)
        return comps

    def _make_complex_block(self, page_no, bboxes, tag):
        x0 = min(b[0] for b in bboxes)
        y0 = min(b[1] for b in bboxes)
        x1 = max(b[2] for b in bboxes)
        y1 = max(b[3] for b in bboxes)
        return {
            "_uid": str(uuid.uuid4()),
            "_origin_id": -1,
            "id": -1,
            "label": "COMPLEX_BLOCK",
            "page": page_no,
            "bbox": [round(x0, 2), round(y0, 2), round(x1, 2), round(y1, 2)],
            "text": tag,
            "text_preview": tag,
        }

    def _make_text_block(self, page_no, bbox, text, label="TEXT", template=None, origin_id=-1):
        item = {
            "_uid": str(uuid.uuid4()),
            "_origin_id": origin_id,
            "id": -1,
            "label": label,
            "page": page_no,
            "bbox": [round(c, 2) for c in bbox],
            "text": text.strip(),
            "text_preview": text[:80].strip(),
        }
        if template:
            for key in (
                "item_type",
                "parent_ref",
                "ancestor_refs",
                "ancestor_labels",
                "ancestor_container_label",
                "level",
                "content_layer",
            ):
                if key in template:
                    item[key] = template[key]
        return item

    def _split_text_item_around_exclude_bbox(self, text_item, exclude_bbox, page_lines, page_rect, word_line_h):
        item_bbox = text_item["bbox"]
        owned_lines = []
        kept_lines = []
        overlapped_lines = 0

        for line in page_lines:
            lb = line["bbox"]
            l_area = self._bbox_area(lb)
            if l_area <= 0:
                continue
            if self._bbox_intersection_area(item_bbox, lb) / l_area < 0.6:
                continue
            owned_lines.append(line)
            overlap = self._bbox_intersection_area(lb, exclude_bbox) / l_area
            center_in_exclude = self._center_in_bbox(self._bbox_center(lb), exclude_bbox)
            if overlap >= 0.18 or center_in_exclude:
                overlapped_lines += 1
                continue
            kept_lines.append(line)

        if overlapped_lines == 0 or len(kept_lines) == len(owned_lines) or not kept_lines:
            return []

        line_clusters = self._cluster_text_lines(
            kept_lines,
            max_vgap=max(word_line_h * 1.25, 10.0),
            x_tolerance=max(page_rect.width * 0.03, word_line_h * 2.0),
        )

        split_items = []
        original_area = max(1.0, self._bbox_area(item_bbox))
        kept_area = 0.0
        for cluster in line_clusters:
            cluster_bbox = self._bbox_union([ln["bbox"] for ln in cluster])
            if self._bbox_overlap_ratio(cluster_bbox, exclude_bbox) >= 0.08:
                continue
            cluster_text = " ".join(ln["text"] for ln in cluster).strip()
            compact = re.sub(r"[\W_]+", "", cluster_text, flags=re.UNICODE)
            if len(cluster) < 2 and len(compact) < 18:
                continue
            kept_area += self._bbox_area(cluster_bbox)
            split_items.append(
                self._make_text_block(
                    text_item["page"],
                    cluster_bbox,
                    cluster_text,
                    label=text_item["label"],
                    template=text_item,
                    origin_id=text_item.get("_origin_id", -1),
                )
            )

        if not split_items or kept_area < original_area * 0.35:
            return []

        split_items.sort(key=lambda it: (it["bbox"][1], it["bbox"][0]))
        merged_items = []
        for item in split_items:
            if not merged_items:
                merged_items.append(item)
                continue

            prev = merged_items[-1]
            prev_box = prev["bbox"]
            curr_box = item["bbox"]
            overlap_x = self._bbox_overlap_x_ratio(prev_box, curr_box)
            vertical_gap = max(0.0, curr_box[1] - prev_box[3])
            vertical_overlap = max(0.0, min(prev_box[3], curr_box[3]) - max(prev_box[1], curr_box[1]))
            should_merge = (
                overlap_x >= 0.6
                and (
                    vertical_overlap > 1.0
                    or vertical_gap <= max(word_line_h * 0.9, 8.0)
                )
            )
            if not should_merge:
                merged_items.append(item)
                continue

            merged_bbox = self._bbox_union([prev_box, curr_box])
            merged_text = " ".join(
                part for part in [(prev.get("text") or "").strip(), (item.get("text") or "").strip()] if part
            ).strip()
            merged_items[-1] = self._make_text_block(
                text_item["page"],
                merged_bbox,
                merged_text,
                label=text_item["label"],
                template=text_item,
                origin_id=text_item.get("_origin_id", -1),
            )

        return merged_items

    def workshop_3_5_formula_complexifier(self):
        """
        阶段2-A.5：公式复杂区域扩选
        目标场景：
        - 只识别到公式编号(1)/(2)而主体漏选
        - 行内/段内矩阵导致 TEXT 与 FORMULA 交错碎裂
        """
        pages = sorted(set(it["page"] for it in self.items))
        remove_uids = set()
        new_items = []
        merged_regions = []  # 防止同页重复造大框
        stats = defaultdict(int)

        for page_no in pages:
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_w, page_h = page_rect.width, page_rect.height
            page_items = [it for it in self.items if it["page"] == page_no]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            formulas = [it for it in page_items if it["label"] == "FORMULA" and it["_uid"] not in remove_uids]
            if not formulas:
                continue

            text_items = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"} and it["_uid"] not in remove_uids]
            text_lines = self._extract_text_lines(page)
            word_line_h = self._estimate_word_line_height(page)
            mask_union = unary_union([box(*it["bbox"]) for it in page_items if it["_uid"] not in remove_uids]) if page_items else None
            text_centers = [((it["bbox"][0] + it["bbox"][2]) / 2.0) for it in text_items]
            text_heights = [it["bbox"][3] - it["bbox"][1] for it in text_items if 6 <= (it["bbox"][3] - it["bbox"][1]) <= 120]
            typical_text_h = median(text_heights) if text_heights else 12.0
            if len(text_centers) >= 4 and (max(text_centers) - min(text_centers)) >= page_w * 0.28:
                col_mid_x = (min(text_centers) + max(text_centers)) / 2.0
            else:
                col_mid_x = page_w / 2.0
            drawings = page.get_drawings() or []
            drawing_boxes = []
            for d in drawings:
                rect = d.get("rect")
                if not rect:
                    continue
                db = [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)]
                if self._bbox_area(db) < 18:
                    continue
                drawing_boxes.append(db)

            def profile_idx_for_bbox(bbox):
                return self._profile_idx_for_bbox(bbox, column_profiles, page_w, word_line_h)

            # Case 1: 明显冲突（FORMULA-FORMULA / FORMULA-TEXT 交集）直接成 complex。
            conflict_nodes = formulas + text_items
            edges = []
            for i in range(len(conflict_nodes)):
                a = conflict_nodes[i]
                a_box = a["bbox"]
                a_area = max(1.0, self._bbox_area(a_box))
                for j in range(i + 1, len(conflict_nodes)):
                    b = conflict_nodes[j]
                    b_box = b["bbox"]
                    inter = self._bbox_intersection_area(a_box, b_box)
                    if inter <= 6.0:
                        continue
                    b_area = max(1.0, self._bbox_area(b_box))
                    overlap_min = inter / min(a_area, b_area)
                    labels = {a["label"], b["label"]}
                    if labels == {"FORMULA"} and overlap_min >= 0.04:
                        edges.append((i, j))
                    elif "FORMULA" in labels and overlap_min >= 0.08:
                        edges.append((i, j))

            for comp in self._components_from_edges(len(conflict_nodes), edges):
                if len(comp) < 2:
                    continue
                members = [conflict_nodes[idx] for idx in comp]
                if not any(m["label"] == "FORMULA" for m in members):
                    continue
                formulas_in_comp = [m for m in members if m["label"] == "FORMULA"]
                comp_bbox = [
                    min(m["bbox"][0] for m in members),
                    min(m["bbox"][1] for m in members),
                    max(m["bbox"][2] for m in members),
                    max(m["bbox"][3] for m in members),
                ]
                # 防止跨双栏误并：若并块过宽，但成员中没有通栏块，则放弃。
                comp_width = comp_bbox[2] - comp_bbox[0]
                has_wide_member = any((m["bbox"][2] - m["bbox"][0]) > page_w * 0.62 for m in members)
                spans_multiple_profiles = self._component_spans_multiple_profiles(
                    [m["bbox"] for m in members],
                    column_profiles,
                    page_w,
                    word_line_h,
                )
                if comp_width > page_w * 0.78 and not has_wide_member:
                    continue
                if spans_multiple_profiles and not has_wide_member:
                    continue
                # 仅吸收真正冲突成员：原始 text 需和某个公式高重叠才可被吸收。
                absorb_members = []
                for m in members:
                    if m["label"] == "FORMULA":
                        absorb_members.append(m)
                        continue
                    if m["label"] not in {"TEXT", "LIST_ITEM", "COMPLEX_BLOCK"}:
                        continue
                    is_raw_text = m.get("_origin_id", -1) > 0 and m["label"] in {"TEXT", "LIST_ITEM"}
                    if not is_raw_text:
                        absorb_members.append(m)
                        continue
                    m_area = max(1.0, self._bbox_area(m["bbox"]))
                    max_ov = 0.0
                    ov_cnt = 0
                    for fm in formulas_in_comp:
                        inter = self._bbox_intersection_area(m["bbox"], fm["bbox"])
                        ov = inter / m_area
                        if ov > max_ov:
                            max_ov = ov
                        if ov >= 0.2:
                            ov_cnt += 1
                    if max_ov >= 0.72 or (ov_cnt >= 2 and max_ov >= 0.45):
                        absorb_members.append(m)

                if len([m for m in absorb_members if m["label"] == "FORMULA"]) == 0:
                    continue
                if len(absorb_members) <= 1:
                    continue
                bboxes = [m["bbox"] for m in absorb_members]
                merged_bbox = [
                    min(b[0] for b in bboxes),
                    min(b[1] for b in bboxes),
                    max(b[2] for b in bboxes),
                    max(b[3] for b in bboxes),
                ]
                if any(self._bbox_iou(merged_bbox, prev) >= 0.55 for prev in merged_regions):
                    continue
                for m in absorb_members:
                    remove_uids.add(m["_uid"])
                new_items.append(self._make_complex_block(page_no, bboxes, "OVERLAP_COMPLEX_REGION"))
                merged_regions.append(merged_bbox)
                stats["overlap_cluster"] += 1

            # Case 2: 编号锚点扩展（只在显著缺失时触发，避免乱撞）。
            anchor_formulas = []
            for formula in formulas:
                if formula["_uid"] in remove_uids:
                    continue
                fx0, fy0, fx1, fy1 = formula["bbox"]
                fw, fh = fx1 - fx0, fy1 - fy0
                f_text = self._item_text(formula)
                if self._looks_like_equation_number(f_text) or (fw < page_w * 0.12 and fh < page_h * 0.08):
                    anchor_formulas.append(formula)

            anchor_edges = []
            for i in range(len(anchor_formulas)):
                a = anchor_formulas[i]["bbox"]
                acx, acy = self._bbox_center(a)
                for j in range(i + 1, len(anchor_formulas)):
                    b = anchor_formulas[j]["bbox"]
                    bcx, bcy = self._bbox_center(b)
                    same_col = (acx < col_mid_x) == (bcx < col_mid_x)
                    if not same_col:
                        continue
                    vertical_gap = max(0.0, max(a[1], b[1]) - min(a[3], b[3]))
                    if vertical_gap > max(word_line_h * 2.8, 22.0):
                        continue
                    if abs(a[2] - b[2]) > page_w * 0.08 and abs(a[0] - b[0]) > page_w * 0.08:
                        continue
                    anchor_edges.append((i, j))

            anchor_groups = self._components_from_edges(len(anchor_formulas), anchor_edges) if anchor_formulas else []
            if not anchor_groups and anchor_formulas:
                anchor_groups = [[idx] for idx in range(len(anchor_formulas))]

            for comp in anchor_groups:
                anchors = [anchor_formulas[idx] for idx in comp if anchor_formulas[idx]["_uid"] not in remove_uids]
                if not anchors:
                    continue

                anchor_boxes = [it["bbox"] for it in anchors]
                fx0 = min(b[0] for b in anchor_boxes)
                fy0 = min(b[1] for b in anchor_boxes)
                fx1 = max(b[2] for b in anchor_boxes)
                fy1 = max(b[3] for b in anchor_boxes)
                fw, fh = fx1 - fx0, fy1 - fy0
                f_area = max(1.0, sum(self._bbox_area(b) for b in anchor_boxes))
                anchor_cx = (fx0 + fx1) / 2.0
                anchor_profile_idx = profile_idx_for_bbox([fx0, fy0, fx1, fy1])
                target_profile = column_profiles[anchor_profile_idx] if anchor_profile_idx is not None and column_profiles else None
                in_left_col = anchor_cx < col_mid_x

                search = self._anchor_search_window(
                    [fx0, fy0, fx1, fy1],
                    page_w,
                    page_h,
                    word_line_h,
                    target_profile,
                    col_mid_x,
                )
                sg = box(*search)
                candidate_boxes = list(anchor_boxes)
                anchor_text_hints = self._collect_anchor_text_hints(
                    page,
                    [fx0, fy0, fx1, fy1],
                    self._item_text(anchors[0]) if len(anchors) == 1 else "",
                    search,
                    word_line_h,
                )
                candidate_boxes.extend(anchor_text_hints)

                local_drawing_boxes = []
                for db in drawing_boxes:
                    d_area = self._bbox_area(db)
                    if d_area <= 0:
                        continue
                    if box(*db).intersection(sg).area / d_area < 0.45:
                        continue
                    db_cx = (db[0] + db[2]) / 2.0
                    if target_profile:
                        profile_pad = max(page_w * 0.02, word_line_h * 1.5)
                        if db_cx < target_profile["x0"] - profile_pad or db_cx > target_profile["x1"] + profile_pad:
                            continue
                    elif in_left_col and db_cx > col_mid_x + page_w * 0.03:
                        continue
                    elif (not in_left_col) and db_cx < col_mid_x - page_w * 0.03:
                        continue
                    d_vertical_gap = max(0.0, max(fy0, db[1]) - min(fy1, db[3]))
                    if d_vertical_gap > max(word_line_h * 4.8, 28.0):
                        continue
                    local_drawing_boxes.append(db)

                drawing_clusters = self._cluster_boxes(
                    local_drawing_boxes,
                    x_pad=max(word_line_h * 0.8, 2.0),
                    y_pad=max(word_line_h * 0.55, 1.5),
                )
                selected_clusters = []
                for cluster in drawing_clusters:
                    cb = cluster["bbox"]
                    vertical_gap = max(0.0, max(fy0, cb[1]) - min(fy1, cb[3]))
                    if vertical_gap > max(word_line_h * 3.4, 22.0):
                        continue
                    if len(cluster["members"]) < 4 and self._bbox_area(cb) < 50.0:
                        continue
                    selected_clusters.append(cluster)

                for cluster in selected_clusters:
                    candidate_boxes.extend(cluster["members"])

                band_drawings = self._collect_anchor_band_drawings(
                    page,
                    search,
                    [fx0, fy0, fx1, fy1],
                    target_profile,
                    page_w,
                    word_line_h,
                )
                candidate_boxes.extend(band_drawings)

                has_vector_support = bool(selected_clusters or band_drawings)
                for line in text_lines:
                    bb = line["bbox"]
                    b_area = self._bbox_area(bb)
                    if b_area <= 0:
                        continue
                    if box(*bb).intersection(sg).area / b_area < 0.25:
                        continue
                    bb_cx = (bb[0] + bb[2]) / 2.0
                    if target_profile:
                        profile_pad = max(page_w * 0.02, word_line_h * 1.5)
                        if bb_cx < target_profile["x0"] - profile_pad or bb_cx > target_profile["x1"] + profile_pad:
                            continue
                    elif in_left_col and bb_cx > col_mid_x + page_w * 0.03:
                        continue
                    elif (not in_left_col) and bb_cx < col_mid_x - page_w * 0.03:
                        continue
                    if mask_union:
                        cov = box(*bb).intersection(mask_union).area / b_area
                        if cov > 0.8:
                            continue
                    text = line["text"]
                    if self._looks_like_folio_token(text, bb, page_rect, word_line_h):
                        continue
                    eq_score = self._equation_signal_score(text)
                    sym_cnt = len(re.findall(r"[\=\+\-\*/\^_<>±×÷∑∏∫√∞∂∇≈≠≤≥\(\)\[\]\{\}|]", text))
                    alnum_cnt = len(re.findall(r"[A-Za-z0-9]", text))
                    sym_ratio = sym_cnt / max(1, alnum_cnt)
                    vertical_gap = max(0.0, max(fy0, bb[1]) - min(fy1, bb[3]))
                    line_width = bb[2] - bb[0]
                    centered_narrow_line = (
                        line_width <= max(page_w * 0.36, fw * 5.8)
                        and abs(bb_cx - anchor_cx) <= page_w * 0.14
                        and vertical_gap <= max(word_line_h * 2.8, 20.0)
                    )
                    eq_like_line = (
                        eq_score >= 2
                        or sym_ratio >= 0.12
                        or self._looks_like_equation_number(text)
                    )
                    if not (eq_like_line or (not has_vector_support and centered_narrow_line)):
                        continue
                    if text and self._looks_like_margin_noise({"bbox": bb, "text_preview": text}, page_rect):
                        continue
                    candidate_boxes.append(bb)

                for other_formula in formulas:
                    if other_formula["_uid"] in remove_uids or other_formula["_uid"] in {it["_uid"] for it in anchors}:
                        continue
                    ob = other_formula["bbox"]
                    same_col = profile_idx_for_bbox(ob) == anchor_profile_idx
                    if not same_col:
                        continue
                    vertical_gap = max(0.0, max(fy0, ob[1]) - min(fy1, ob[3]))
                    if vertical_gap > max(word_line_h * 2.8, 22.0):
                        continue
                    if ob[0] > search[2] + page_w * 0.03 or ob[2] < search[0] - page_w * 0.03:
                        continue
                    candidate_boxes.append(ob)

                if len(candidate_boxes) <= len(anchor_boxes):
                    continue

                merged_bbox = [
                    min(cb[0] for cb in candidate_boxes),
                    min(cb[1] for cb in candidate_boxes),
                    max(cb[2] for cb in candidate_boxes),
                    max(cb[3] for cb in candidate_boxes),
                ]
                if len(column_profiles) >= 2:
                    target_profile = min(
                        column_profiles,
                        key=lambda p: abs(((p["x0"] + p["x1"]) / 2.0) - anchor_cx),
                    )
                    column_pad = max(page_w * 0.055, word_line_h * 2.4)
                    if (
                        merged_bbox[0] < target_profile["x0"] - column_pad
                        or merged_bbox[2] > target_profile["x1"] + column_pad
                    ):
                        continue
                merged_area = self._bbox_area(merged_bbox)
                if merged_area < f_area * 2.0 and (merged_bbox[2] - merged_bbox[0]) < fw + page_w * 0.08:
                    continue
                if any(self._bbox_iou(merged_bbox, prev) >= 0.55 for prev in merged_regions):
                    continue

                touched = list(anchors)
                touched_uids = {it["_uid"] for it in anchors}
                for it in page_items:
                    if it["_uid"] in remove_uids or it["_uid"] in touched_uids:
                        continue
                    if it["label"] not in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "FORMULA", "COMPLEX_BLOCK"}:
                        continue
                    inter = self._bbox_intersection_area(it["bbox"], merged_bbox)
                    if inter <= 0:
                        continue
                    it_area = max(1.0, self._bbox_area(it["bbox"]))
                    cx, cy = self._bbox_center(it["bbox"])
                    center_in = merged_bbox[0] <= cx <= merged_bbox[2] and merged_bbox[1] <= cy <= merged_bbox[3]
                    if it["label"] == "FORMULA":
                        same_col = profile_idx_for_bbox(it["bbox"]) == anchor_profile_idx
                        if not same_col:
                            continue
                        if (inter / it_area) >= 0.08 or center_in:
                            touched.append(it)
                            touched_uids.add(it["_uid"])
                        continue
                    if it.get("_origin_id", -1) > 0 and it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER"}:
                        text_width = it["bbox"][2] - it["bbox"][0]
                        if text_width > max(page_w * 0.42, fw * 6.5) and (inter / it_area) < 0.72:
                            continue
                        if (inter / it_area) < 0.6:
                            continue
                    if (inter / it_area) >= 0.25 or center_in:
                        touched.append(it)
                        touched_uids.add(it["_uid"])
                anchor_only_expansion = (
                    merged_area >= f_area * 3.5
                    or (merged_bbox[2] - merged_bbox[0]) >= fw + page_w * 0.08
                    or (merged_bbox[3] - merged_bbox[1]) >= fh + word_line_h * 1.8
                )
                if len(touched) <= len(anchors) and not anchor_only_expansion:
                    continue

                for t in touched:
                    remove_uids.add(t["_uid"])
                touch_boxes = [t["bbox"] for t in touched] + [merged_bbox]
                new_items.append(self._make_complex_block(page_no, touch_boxes, "NUMBER_ANCHOR_COMPLEX"))
                merged_regions.append(merged_bbox)
                stats["number_anchor"] += 1

            # Case 2.5: 孤立编号锚点 fallback。
            # 处理“只有编号残留、3.5 主流程未造出 complex，但周边矢量公式证据很强”的场景。
            for formula in anchor_formulas:
                if formula["_uid"] in remove_uids:
                    continue
                if any(
                    self._bbox_overlap_ratio(formula["bbox"], prev) >= 0.35
                    or self._center_in_bbox(self._bbox_center(formula["bbox"]), prev)
                    for prev in merged_regions
                ):
                    continue

                fx0, fy0, fx1, fy1 = formula["bbox"]
                fw, fh = fx1 - fx0, fy1 - fy0
                anchor_cx = (fx0 + fx1) / 2.0
                in_left_col = anchor_cx < col_mid_x
                search = self._anchor_search_window(
                    [fx0, fy0, fx1, fy1],
                    page_w,
                    page_h,
                    word_line_h,
                    None,
                    col_mid_x,
                )
                local_drawing_boxes = []
                for db in drawing_boxes:
                    d_area = self._bbox_area(db)
                    if d_area <= 0:
                        continue
                    if box(*db).intersection(box(*search)).area / d_area < 0.45:
                        continue
                    db_cx = (db[0] + db[2]) / 2.0
                    if in_left_col and db_cx > col_mid_x + page_w * 0.03:
                        continue
                    if (not in_left_col) and db_cx < col_mid_x - page_w * 0.03:
                        continue
                    vertical_gap = max(0.0, max(fy0, db[1]) - min(fy1, db[3]))
                    if vertical_gap > max(word_line_h * 5.0, 34.0):
                        continue
                    local_drawing_boxes.append(db)

                if len(local_drawing_boxes) < 4:
                    continue

                drawing_clusters = self._cluster_boxes(
                    local_drawing_boxes,
                    x_pad=max(word_line_h * 0.85, 2.0),
                    y_pad=max(word_line_h * 0.6, 1.6),
                )
                candidate_boxes = [formula["bbox"]]
                candidate_boxes.extend(
                    self._collect_anchor_text_hints(
                        page,
                        formula["bbox"],
                        self._item_text(formula),
                        search,
                        word_line_h,
                    )
                )
                vector_support_area = 0.0
                for cluster in drawing_clusters:
                    cb = cluster["bbox"]
                    vertical_gap = max(0.0, max(fy0, cb[1]) - min(fy1, cb[3]))
                    if vertical_gap > max(word_line_h * 3.8, 26.0):
                        continue
                    if len(cluster["members"]) < 4 and self._bbox_area(cb) < 80.0:
                        continue
                    vector_support_area += self._bbox_area(cb)
                    candidate_boxes.extend(cluster["members"])

                band_drawings = self._collect_anchor_band_drawings(
                    page,
                    search,
                    formula["bbox"],
                    target_profile,
                    page_w,
                    word_line_h,
                )
                if band_drawings:
                    candidate_boxes.extend(band_drawings)
                    vector_support_area += sum(self._bbox_area(db) for db in band_drawings)

                merged_bbox = [
                    min(cb[0] for cb in candidate_boxes),
                    min(cb[1] for cb in candidate_boxes),
                    max(cb[2] for cb in candidate_boxes),
                    max(cb[3] for cb in candidate_boxes),
                ]
                merged_width = merged_bbox[2] - merged_bbox[0]
                merged_height = merged_bbox[3] - merged_bbox[1]
                if len(column_profiles) >= 2:
                    target_profile = column_profiles[anchor_profile_idx]
                    column_pad = max(page_w * 0.055, word_line_h * 2.4)
                    if (
                        merged_bbox[0] < target_profile["x0"] - column_pad
                        or merged_bbox[2] > target_profile["x1"] + column_pad
                    ):
                        continue
                if vector_support_area < max(self._bbox_area(formula["bbox"]) * 8.0, 1200.0):
                    continue
                if merged_width < max(page_w * 0.16, fw * 4.5) or merged_height < max(word_line_h * 2.2, 28.0):
                    continue
                if any(self._bbox_iou(merged_bbox, prev) >= 0.42 for prev in merged_regions):
                    continue

                remove_uids.add(formula["_uid"])
                new_items.append(self._make_complex_block(page_no, candidate_boxes, "NUMBER_ANCHOR_COMPLEX"))
                merged_regions.append(merged_bbox)
                stats["number_anchor_fallback"] += 1

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 3.5] 公式复杂区域扩选完成，"
            f"新增 COMPLEX_BLOCK {len(new_items)} 个，移除冲突块 {len(remove_uids)} 个，"
            f"明细: {dict(stats)}"
        )

    def workshop_3_6_complex_closure(self):
        """
        阶段2-A.6：complex 传递闭包合并
        目标场景：
        - FORMULA 与一部分 TEXT 合并成 complex 后，complex 继续与相交的 TEXT 合并
        - 由行内公式切裂出的多个重叠 TEXT 块，通过公式/complex 形成传递闭包
        """
        pages = sorted(set(it["page"] for it in self.items))
        remove_uids = set()
        new_items = []
        stats = defaultdict(int)

        for page_no in pages:
            page_rect = self.doc[page_no - 1].rect
            page_w = page_rect.width
            word_line_h = self._estimate_word_line_height(self.doc[page_no - 1])
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in remove_uids]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            nodes = [
                it
                for it in page_items
                if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "FORMULA", "COMPLEX_BLOCK"}
            ]
            if len(nodes) < 2:
                continue

            edges = []
            for i in range(len(nodes)):
                a = nodes[i]
                a_box = a["bbox"]
                a_area = max(1.0, self._bbox_area(a_box))
                ac = self._bbox_center(a_box)

                for j in range(i + 1, len(nodes)):
                    b = nodes[j]
                    b_box = b["bbox"]
                    inter = self._bbox_intersection_area(a_box, b_box)
                    b_area = max(1.0, self._bbox_area(b_box))
                    if inter > 4.0:
                        overlap_min = inter / min(a_area, b_area)
                        overlap_a = inter / a_area
                        overlap_b = inter / b_area
                    else:
                        overlap_min = 0.0
                        overlap_a = 0.0
                        overlap_b = 0.0
                    overlap_x = max(0.0, min(a_box[2], b_box[2]) - max(a_box[0], b_box[0])) / max(
                        1.0, min(a_box[2] - a_box[0], b_box[2] - b_box[0])
                    )
                    vertical_gap = max(0.0, max(a_box[1], b_box[1]) - min(a_box[3], b_box[3]))
                    bc = self._bbox_center(b_box)
                    a_in_b = self._center_in_bbox(ac, b_box)
                    b_in_a = self._center_in_bbox(bc, a_box)
                    labels = {a["label"], b["label"]}

                    connect = False
                    if "COMPLEX_BLOCK" in labels:
                        if labels == {"COMPLEX_BLOCK"}:
                            connect = overlap_min >= 0.16 or a_in_b or b_in_a
                        else:
                            connect = overlap_min >= 0.08 or overlap_a >= 0.2 or overlap_b >= 0.2 or a_in_b or b_in_a
                    elif "FORMULA" in labels:
                        connect = overlap_min >= 0.08 or overlap_a >= 0.18 or overlap_b >= 0.18 or a_in_b or b_in_a
                    else:
                        # 仅在明显重叠/包含时连接 text-text，避免把邻近正文段误并。
                        similar_scale = max(a_area, b_area) <= min(a_area, b_area) * 2.6
                        connect = (overlap_min >= 0.14 or a_in_b or b_in_a) and similar_scale

                    if not connect and ("FORMULA" in labels or "COMPLEX_BLOCK" in labels):
                        text_like_labels = {"TEXT", "LIST_ITEM", "SECTION_HEADER"}
                        text_node = None
                        if a["label"] in text_like_labels:
                            text_node = a
                        elif b["label"] in text_like_labels:
                            text_node = b

                        formula_stack_text = False
                        if text_node is not None:
                            formula_stack_text = self._formula_stack_text_bridgeable(
                                text_node,
                                nodes,
                                page_w,
                                word_line_h,
                            )

                        connect = (
                            overlap_x >= 0.55
                            and vertical_gap <= max(word_line_h * 1.8, 20.0)
                            and (
                                labels.issubset({"FORMULA", "COMPLEX_BLOCK"})
                                or formula_stack_text
                            )
                        )

                    if connect:
                        edges.append((i, j))

            if not edges:
                continue

            merged_regions = []
            for comp in self._components_from_edges(len(nodes), edges):
                if len(comp) < 2:
                    continue

                members = [nodes[idx] for idx in comp if nodes[idx]["_uid"] not in remove_uids]
                if len(members) < 2:
                    continue
                if not any(m["label"] in {"FORMULA", "COMPLEX_BLOCK"} for m in members):
                    continue

                comp_bbox = [
                    min(m["bbox"][0] for m in members),
                    min(m["bbox"][1] for m in members),
                    max(m["bbox"][2] for m in members),
                    max(m["bbox"][3] for m in members),
                ]
                comp_width = comp_bbox[2] - comp_bbox[0]
                has_wide_member = any((m["bbox"][2] - m["bbox"][0]) > page_w * 0.62 for m in members)
                spans_multiple_profiles = self._component_spans_multiple_profiles(
                    [m["bbox"] for m in members],
                    column_profiles,
                    page_w,
                    word_line_h,
                )
                if comp_width > page_w * 0.82 and not has_wide_member:
                    continue
                if spans_multiple_profiles and not has_wide_member:
                    continue
                if any(self._bbox_iou(comp_bbox, prev) >= 0.58 for prev in merged_regions):
                    continue

                closure_tag = "COMPLEX_CLOSURE"
                if any(
                    m["label"] == "FORMULA"
                    or self._is_formula_like_complex(m)
                    for m in members
                ):
                    closure_tag = "FORMULA_COMPLEX_CLOSURE"

                for m in members:
                    remove_uids.add(m["_uid"])
                new_items.append(self._make_complex_block(page_no, [m["bbox"] for m in members], closure_tag))
                merged_regions.append(comp_bbox)
                stats["closure_merge"] += 1
                stats["absorbed_nodes"] += len(members)

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 3.6] complex 传递闭包完成，"
            f"新增 COMPLEX_BLOCK {len(new_items)} 个，移除节点 {len(remove_uids)} 个，"
            f"明细: {dict(stats)}"
        )

    def workshop_3_7_formula_complex_absorber(self):
        """
        阶段2-A.7：formula-like complex 重叠吸收
        目标场景：
        - complex 与 picture/table/text 等仍有实质交叠
        - 遵循“有交集就扩大成一个 complex”的原则，但仅作用于公式型 complex
        """
        pages = sorted(set(it["page"] for it in self.items))
        remove_uids = set()
        new_items = []
        stats = defaultdict(int)

        for page_no in pages:
            page_rect = self.doc[page_no - 1].rect
            page_w = page_rect.width
            word_line_h = self._estimate_word_line_height(self.doc[page_no - 1])
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in remove_uids]
            seeds = [it for it in page_items if self._is_formula_like_complex(it)]
            if not seeds:
                continue

            candidate_labels = {"TEXT", "LIST_ITEM", "SECTION_HEADER", "FORMULA", "COMPLEX_BLOCK", "PICTURE", "TABLE"}
            consumed = set()
            merged_regions = []

            for seed in seeds:
                if seed["_uid"] in consumed:
                    continue

                cluster = [seed]
                cluster_uids = {seed["_uid"]}
                cluster_bbox = list(seed["bbox"])

                changed = True
                while changed:
                    changed = False
                    for other in page_items:
                        if other["_uid"] in cluster_uids or other["_uid"] in consumed:
                            continue
                        if other["label"] not in candidate_labels:
                            continue
                        if not self._should_absorb_into_formula_complex(cluster_bbox, other, page_w, word_line_h):
                            continue
                        cluster.append(other)
                        cluster_uids.add(other["_uid"])
                        cluster_bbox = self._bbox_union([cluster_bbox, other["bbox"]])
                        changed = True

                if len(cluster) <= 1:
                    continue
                if any(self._bbox_iou(cluster_bbox, prev) >= 0.7 for prev in merged_regions):
                    continue

                for member in cluster:
                    remove_uids.add(member["_uid"])
                    consumed.add(member["_uid"])

                new_items.append(
                    self._make_complex_block(page_no, [m["bbox"] for m in cluster], "FORMULA_COMPLEX_ABSORB")
                )
                merged_regions.append(cluster_bbox)
                stats["absorbed_clusters"] += 1
                stats["absorbed_nodes"] += len(cluster)

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 3.7] formula-like complex 重叠吸收完成，"
            f"新增 COMPLEX_BLOCK {len(new_items)} 个，移除节点 {len(remove_uids)} 个，"
            f"明细: {dict(stats)}"
        )

    def workshop_3_75_formula_complex_drawing_extender(self):
        """
        阶段2-A.75：formula-like complex drawing 尾巴扩展
        目标场景：
        - 公式 complex 已形成，但末尾/开头仍漏了一行紧邻的 drawing 公式
        - 不改动正常正文，只对紧贴 complex 的矢量公式尾巴做补齐
        """
        remove_uids = set()
        new_items = []
        stats = defaultdict(int)

        for page_no in range(1, len(self.doc) + 1):
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_w = page_rect.width
            word_line_h = self._estimate_word_line_height(page)
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in remove_uids]
            seeds = [it for it in page_items if self._is_formula_like_complex(it)]
            if not seeds:
                continue

            raw_boxes = []
            for d in page.get_drawings() or []:
                rect = d.get("rect")
                if not rect:
                    continue
                db = [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)]
                if self._bbox_area(db) < 4.0:
                    continue
                raw_boxes.append(db)
            if not raw_boxes:
                continue

            clusters = self._cluster_boxes(
                raw_boxes,
                x_pad=max(word_line_h * 0.42, 1.4),
                y_pad=max(word_line_h * 0.32, 0.9),
            )
            if not clusters:
                continue

            for seed in seeds:
                if seed["_uid"] in remove_uids:
                    continue
                cb = seed["bbox"]
                x_pad = max(page_w * 0.04, word_line_h * 3.2)
                y_pad = max(word_line_h * 1.8, 18.0)
                probe = [cb[0] - x_pad, cb[1] - y_pad, cb[2] + x_pad, cb[3] + y_pad]
                selected_members = []

                for cluster in clusters:
                    kb = cluster["bbox"]
                    k_area = max(1.0, self._bbox_area(kb))
                    if self._bbox_intersection_area(kb, probe) / k_area < 0.2:
                        continue
                    overlap_x = self._bbox_overlap_x_ratio(kb, cb)
                    if overlap_x < 0.5:
                        continue
                    vertical_gap = max(0.0, max(cb[1], kb[1]) - min(cb[3], kb[3]))
                    if vertical_gap > max(word_line_h * 1.8, 20.0):
                        continue
                    expanded_bbox = self._bbox_union([cb, kb])
                    if (expanded_bbox[3] - expanded_bbox[1]) > (cb[3] - cb[1]) + max(word_line_h * 3.0, 28.0):
                        continue
                    selected_members.extend(cluster["members"])

                if not selected_members:
                    continue

                expanded_bbox = self._bbox_union([cb] + selected_members)
                if self._bbox_iou(expanded_bbox, cb) >= 0.995:
                    continue

                absorb_members = [cb] + selected_members
                absorb_uids = {seed["_uid"]}
                absorb_bbox = list(expanded_bbox)
                changed = True
                while changed:
                    changed = False
                    for other in page_items:
                        if other["_uid"] in absorb_uids or other["_uid"] in remove_uids:
                            continue
                        if other["label"] not in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "FORMULA"}:
                            continue
                        if not self._should_absorb_into_formula_complex(absorb_bbox, other, page_w, word_line_h):
                            continue
                        absorb_members.append(other["bbox"])
                        absorb_uids.add(other["_uid"])
                        absorb_bbox = self._bbox_union([absorb_bbox, other["bbox"]])
                        changed = True

                remove_uids.add(seed["_uid"])
                remove_uids.update(uid for uid in absorb_uids if uid != seed["_uid"])
                new_items.append(
                    self._make_complex_block(
                        page_no,
                        absorb_members,
                        seed.get("text") or "FORMULA_COMPLEX_EXTEND",
                    )
                )
                stats["extended"] += 1
                stats["absorbed_overlap_nodes"] += max(0, len(absorb_uids) - 1)

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 3.75] formula-like complex drawing 尾巴扩展完成，"
            f"扩展 COMPLEX_BLOCK {stats['extended']} 个；"
            f"追加吸收节点 {stats['absorbed_overlap_nodes']} 个。"
        )

    def workshop_3_45_solitary_formula_anchor_fallback(self):
        """
        阶段2-A.45：孤立编号公式 fallback
        目标场景：
        - Docling 只留下 (9)/(12) 之类编号锚点
        - 周边没有足够 docling 文本可吸收，但 PDF 矢量笔画已经明显构成一个公式区
        """
        pages = sorted(set(it["page"] for it in self.items))
        remove_uids = set()
        new_items = []
        stats = defaultdict(int)

        for page_no in pages:
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_w, page_h = page_rect.width, page_rect.height
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in remove_uids]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            text_items = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"}]
            if len(text_items) < 2:
                continue

            text_centers = [self._bbox_center(it["bbox"])[0] for it in text_items]
            if len(text_centers) >= 4 and (max(text_centers) - min(text_centers)) >= page_w * 0.28:
                col_mid_x = (min(text_centers) + max(text_centers)) / 2.0
            else:
                col_mid_x = page_w / 2.0

            drawings = page.get_drawings() or []
            drawing_boxes = []
            for d in drawings:
                rect = d.get("rect")
                if not rect:
                    continue
                db = [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)]
                if self._bbox_area(db) < 18.0:
                    continue
                drawing_boxes.append(db)

            def profile_idx_for_bbox(bbox):
                return self._profile_idx_for_bbox(bbox, column_profiles, page_w, word_line_h)

            word_line_h = self._estimate_word_line_height(page)
            formulas = [
                it
                for it in page_items
                if it["label"] == "FORMULA"
                and (
                    self._looks_like_equation_number(self._item_text(it))
                    or (
                        (it["bbox"][2] - it["bbox"][0]) < page_w * 0.12
                        and (it["bbox"][3] - it["bbox"][1]) < page_h * 0.08
                    )
                )
            ]
            complexes = [it for it in page_items if it["label"] == "COMPLEX_BLOCK"]
            merged_regions = [it["bbox"] for it in complexes]

            for formula in formulas:
                if formula["_uid"] in remove_uids:
                    continue
                if any(
                    self._bbox_overlap_ratio(formula["bbox"], prev) >= 0.3
                    or self._center_in_bbox(self._bbox_center(formula["bbox"]), prev)
                    for prev in merged_regions
                ):
                    continue

                fx0, fy0, fx1, fy1 = formula["bbox"]
                fw, fh = fx1 - fx0, fy1 - fy0
                anchor_cx = (fx0 + fx1) / 2.0
                anchor_profile_idx = profile_idx_for_bbox([fx0, fy0, fx1, fy1])
                target_profile = column_profiles[anchor_profile_idx] if anchor_profile_idx is not None and column_profiles else None
                in_left_col = anchor_cx < col_mid_x
                search = self._anchor_search_window(
                    [fx0, fy0, fx1, fy1],
                    page_w,
                    page_h,
                    word_line_h,
                    target_profile,
                    col_mid_x,
                )

                local_drawing_boxes = []
                for db in drawing_boxes:
                    d_area = self._bbox_area(db)
                    if d_area <= 0:
                        continue
                    if box(*db).intersection(box(*search)).area / d_area < 0.45:
                        continue
                    db_cx = (db[0] + db[2]) / 2.0
                    if target_profile:
                        profile_pad = max(page_w * 0.02, word_line_h * 1.5)
                        if db_cx < target_profile["x0"] - profile_pad or db_cx > target_profile["x1"] + profile_pad:
                            continue
                    elif in_left_col and db_cx > col_mid_x + page_w * 0.03:
                        continue
                    elif (not in_left_col) and db_cx < col_mid_x - page_w * 0.03:
                        continue
                    vertical_gap = max(0.0, max(fy0, db[1]) - min(fy1, db[3]))
                    if vertical_gap > max(word_line_h * 5.0, 34.0):
                        continue
                    local_drawing_boxes.append(db)

                if len(local_drawing_boxes) < 5:
                    continue

                drawing_clusters = self._cluster_boxes(
                    local_drawing_boxes,
                    x_pad=max(word_line_h * 0.85, 2.0),
                    y_pad=max(word_line_h * 0.6, 1.6),
                )
                candidate_boxes = [formula["bbox"]]
                vector_support_area = 0.0
                member_count = 1
                for cluster in drawing_clusters:
                    cb = cluster["bbox"]
                    vertical_gap = max(0.0, max(fy0, cb[1]) - min(fy1, cb[3]))
                    if vertical_gap > max(word_line_h * 4.0, 28.0):
                        continue
                    if len(cluster["members"]) < 4 and self._bbox_area(cb) < 80.0:
                        continue
                    candidate_boxes.extend(cluster["members"])
                    vector_support_area += self._bbox_area(cb)
                    member_count += len(cluster["members"])

                if member_count <= 6:
                    continue

                merged_bbox = [
                    min(cb[0] for cb in candidate_boxes),
                    min(cb[1] for cb in candidate_boxes),
                    max(cb[2] for cb in candidate_boxes),
                    max(cb[3] for cb in candidate_boxes),
                ]
                merged_width = merged_bbox[2] - merged_bbox[0]
                merged_height = merged_bbox[3] - merged_bbox[1]
                if vector_support_area < max(self._bbox_area(formula["bbox"]) * 8.0, 1200.0):
                    continue
                if merged_width < max(page_w * 0.16, fw * 4.5) or merged_height < max(word_line_h * 2.2, 28.0):
                    continue
                if len(column_profiles) >= 2:
                    target_profile = column_profiles[anchor_profile_idx]
                    column_pad = max(page_w * 0.055, word_line_h * 2.4)
                    if (
                        merged_bbox[0] < target_profile["x0"] - column_pad
                        or merged_bbox[2] > target_profile["x1"] + column_pad
                    ):
                        continue
                if any(self._bbox_iou(merged_bbox, prev) >= 0.42 for prev in merged_regions):
                    continue

                remove_uids.add(formula["_uid"])
                new_items.append(self._make_complex_block(page_no, candidate_boxes, "NUMBER_ANCHOR_COMPLEX"))
                merged_regions.append(merged_bbox)
                stats["solitary_anchor"] += 1

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 3.45] 孤立编号公式 fallback 完成，"
            f"新增 COMPLEX_BLOCK {len(new_items)} 个，移除锚点 {len(remove_uids)} 个，"
            f"明细: {dict(stats)}"
        )

    def workshop_3_55_formula_stack_merger(self):
        """
        阶段2-A.55：公式栈合并
        目标场景：
        - 同一列中多个公式/已有 complex 与一小段公式说明文字交错堆叠
        - Docling 将 (20a)(20b)... 与说明文字切碎，但整体应作为复杂块交给图像链路
        """
        pages = sorted(set(it["page"] for it in self.items))
        remove_uids = set()
        new_items = []
        stats = defaultdict(int)

        for page_no in pages:
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_w = page_rect.width
            word_line_h = self._estimate_word_line_height(page)
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in remove_uids]
            if len(page_items) < 3:
                continue

            page_blocks = []
            for block in page.get_text("blocks"):
                if len(block) < 7 or block[6] != 0:
                    continue
                bb = [float(block[0]), float(block[1]), float(block[2]), float(block[3])]
                txt = (block[4] or "").strip().replace("\n", " ")
                if not txt:
                    continue
                eq_like = (
                    self._looks_like_equation_number(txt)
                    or self._equation_signal_score(txt) >= 1
                    or bool(re.search(r"\(\s*\d+[a-z]?\s*\)", txt.lower()))
                )
                page_blocks.append({"bbox": bb, "text": txt, "eq_like": eq_like})

            text_centers = [
                self._bbox_center(it["bbox"])[0]
                for it in page_items
                if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "COMPLEX_BLOCK"}
            ]
            if len(text_centers) >= 4 and (max(text_centers) - min(text_centers)) >= page_w * 0.28:
                col_mid_x = (min(text_centers) + max(text_centers)) / 2.0
            else:
                col_mid_x = page_w / 2.0

            nodes = [
                it
                for it in page_items
                if it["label"] in {"TEXT", "LIST_ITEM", "FORMULA", "COMPLEX_BLOCK"}
            ]
            if len(nodes) < 3:
                continue

            node_block_support = {}
            for node in nodes:
                supported = False
                if node["label"] in {"TEXT", "LIST_ITEM"}:
                    n_area = max(1.0, self._bbox_area(node["bbox"]))
                    for blk in page_blocks:
                        inter = self._bbox_intersection_area(node["bbox"], blk["bbox"])
                        if inter / n_area >= 0.58 and blk["eq_like"]:
                            supported = True
                            break
                node_block_support[node["_uid"]] = supported

            edges = []
            for i in range(len(nodes)):
                a = nodes[i]
                a_box = a["bbox"]
                a_cx = self._bbox_center(a_box)[0]
                for j in range(i + 1, len(nodes)):
                    b = nodes[j]
                    b_box = b["bbox"]
                    b_cx = self._bbox_center(b_box)[0]
                    same_col = (a_cx < col_mid_x) == (b_cx < col_mid_x)
                    if not same_col:
                        continue

                    overlap_x = max(0.0, min(a_box[2], b_box[2]) - max(a_box[0], b_box[0])) / max(
                        1.0, min(a_box[2] - a_box[0], b_box[2] - b_box[0])
                    )
                    if overlap_x < 0.55:
                        continue
                    vertical_gap = max(0.0, max(a_box[1], b_box[1]) - min(a_box[3], b_box[3]))
                    if vertical_gap > max(word_line_h * 1.8, 20.0):
                        continue

                    labels = {a["label"], b["label"]}
                    if labels.issubset({"FORMULA", "COMPLEX_BLOCK"}):
                        edges.append((i, j))
                        continue

                    text_node = a if a["label"] in {"TEXT", "LIST_ITEM"} else b if b["label"] in {"TEXT", "LIST_ITEM"} else None
                    if text_node is None:
                        continue
                    t_height = text_node["bbox"][3] - text_node["bbox"][1]
                    if t_height > max(word_line_h * 5.0, 56.0):
                        continue
                    if not node_block_support.get(text_node["_uid"], False):
                        continue
                    edges.append((i, j))

            if not edges:
                continue

            merged_regions = []
            for comp in self._components_from_edges(len(nodes), edges):
                members = [nodes[idx] for idx in comp if nodes[idx]["_uid"] not in remove_uids]
                if len(members) < 3:
                    continue
                formula_like_count = sum(1 for m in members if m["label"] in {"FORMULA", "COMPLEX_BLOCK"})
                text_count = sum(1 for m in members if m["label"] in {"TEXT", "LIST_ITEM"})
                if formula_like_count < 2 or text_count < 1:
                    continue

                comp_bbox = [
                    min(m["bbox"][0] for m in members),
                    min(m["bbox"][1] for m in members),
                    max(m["bbox"][2] for m in members),
                    max(m["bbox"][3] for m in members),
                ]
                comp_width = comp_bbox[2] - comp_bbox[0]
                if comp_width < page_w * 0.24:
                    continue
                if any(self._bbox_iou(comp_bbox, prev) >= 0.58 for prev in merged_regions):
                    continue

                for m in members:
                    remove_uids.add(m["_uid"])
                new_items.append(self._make_complex_block(page_no, [m["bbox"] for m in members], "FORMULA_STACK_COMPLEX"))
                merged_regions.append(comp_bbox)
                stats["stack_merge"] += 1
                stats["absorbed_nodes"] += len(members)

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 3.55] 公式栈合并完成，"
            f"新增 COMPLEX_BLOCK {len(new_items)} 个，移除节点 {len(remove_uids)} 个，"
            f"明细: {dict(stats)}"
        )

    def workshop_4_targeted_gap_filler(self):
        """阶段2-B：定向补缺（仅补列内缺段，不再做全页重建）"""
        new_items = []
        pages = range(1, len(self.doc) + 1)
        debug_counter = defaultdict(int)

        for page_no in pages:
            page = self.doc[page_no - 1]
            page_items = [it for it in self.items if it["page"] == page_no]
            if not page_items:
                continue

            page_rect = page.rect
            page_masks = [box(*it["bbox"]) for it in page_items]
            mask_union = unary_union(page_masks) if page_masks else None
            body_env = self._build_body_envelope(page_items, page_rect)

            page_containers = [
                it
                for it in page_items
                if it["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC", "PICTURE"}
            ]
            text_like_labels = {"TEXT", "LIST_ITEM", "SECTION_HEADER", "TITLE", "FOOTNOTE", "COMPLEX_BLOCK"}
            page_text_like = [it for it in page_items if it["label"] in text_like_labels]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            if not column_profiles:
                debug_counter["no_columns"] += 1
                continue
            wide_restart_blocks = [
                it
                for it in page_items
                if (it["bbox"][2] - it["bbox"][0]) >= page_rect.width * 0.66
                and it["label"] in {"PICTURE", "TABLE", "CAPTION", "COMPLEX_BLOCK"}
            ]

            page_existing_bboxes = [it["bbox"] for it in page_items]
            page_new_bboxes = []
            text_existing_bboxes = [it["bbox"] for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"}]
            existing_sigs = {
                self._normalize_signature(self._item_text(it))
                for it in page_text_like
                if len(self._normalize_signature(self._item_text(it))) >= 8
            }
            text_heights = [it["bbox"][3] - it["bbox"][1] for it in page_text_like if 6 <= (it["bbox"][3] - it["bbox"][1]) <= 120]
            typical_text_h = median(text_heights) if text_heights else 12.0
            word_line_h = self._estimate_word_line_height(page)
            page_blocks = [b for b in page.get_text("blocks") if len(b) >= 7 and b[6] == 0]

            def overlap_x_ratio(a, b):
                return self._bbox_overlap_x_ratio(a, b)

            if len(column_profiles) == 1:
                block_hints = []
                for b in page_blocks:
                    bb = [float(b[0]), float(b[1]), float(b[2]), float(b[3])]
                    txt = (b[4] or "").strip().replace("\n", " ")
                    if len(txt) < 24:
                        continue
                    bw = bb[2] - bb[0]
                    bh = bb[3] - bb[1]
                    if bw < page_rect.width * 0.22 or bw > page_rect.width * 0.52:
                        continue
                    if bh < max(typical_text_h * 0.9, 40.0):
                        continue
                    if self._looks_like_margin_noise({"bbox": bb, "text": txt, "text_preview": txt}, page_rect):
                        continue
                    if any(
                        self._bbox_overlap_ratio(bb, cont["bbox"]) >= 0.25 or self._center_in_bbox(self._bbox_center(bb), cont["bbox"])
                        for cont in page_containers
                    ):
                        continue
                    block_hints.append({"bbox": bb, "text": txt})

                if block_hints:
                    existing = column_profiles[0]
                    existing_cx = (existing["x0"] + existing["x1"]) / 2.0
                    left_hints = [bh for bh in block_hints if self._bbox_center(bh["bbox"])[0] < page_rect.width / 2.0]
                    right_hints = [bh for bh in block_hints if self._bbox_center(bh["bbox"])[0] >= page_rect.width / 2.0]
                    opposite_hints = right_hints if existing_cx < page_rect.width / 2.0 else left_hints
                    if opposite_hints:
                        xs0 = sorted(h["bbox"][0] for h in opposite_hints)
                        xs1 = sorted(h["bbox"][2] for h in opposite_hints)
                        ys0 = sorted(h["bbox"][1] for h in opposite_hints)
                        widths = sorted((h["bbox"][2] - h["bbox"][0]) for h in opposite_hints)
                        synth = {
                            "items": [{"bbox": h["bbox"], "label": "BLOCK_HINT"} for h in opposite_hints],
                            "x0": self._percentile(xs0, 0.15),
                            "x1": self._percentile(xs1, 0.85),
                            "y0": self._percentile(ys0, 0.1),
                            "median_width": self._percentile(widths, 0.5),
                            "hint_only": True,
                        }
                        column_profiles.append(synth)

            def pick_profile(candidate_bbox):
                cx, _ = self._bbox_center(candidate_bbox)
                x_pad = max(page_rect.width * 0.03, typical_text_h * 2.0)
                matches = []
                for profile in column_profiles:
                    if cx < profile["x0"] - x_pad or cx > profile["x1"] + x_pad:
                        continue
                    cand_overlap = max(
                        (overlap_x_ratio(candidate_bbox, ref["bbox"]) for ref in profile["items"]),
                        default=0.0,
                    )
                    matches.append((cand_overlap, profile))
                if not matches:
                    return None
                matches.sort(key=lambda x: x[0], reverse=True)
                return matches[0][1]

            def neighbor_support(candidate_bbox, refs):
                x0, y0, x1, y1 = candidate_bbox
                support = 0
                for ref in refs:
                    ref_bbox = ref["bbox"]
                    rx0, ry0, rx1, ry1 = ref_bbox
                    overlap_ratio = overlap_x_ratio(candidate_bbox, ref_bbox)
                    if overlap_ratio < 0.24:
                        continue
                    v_gap = min(abs(y0 - ry1), abs(ry0 - y1))
                    if v_gap <= typical_text_h * 2.8:
                        support += 1
                        if support >= 2:
                            return support
                return support

            for block in page.get_text("blocks"):
                if len(block) < 7 or block[6] != 0:
                    continue

                bbox = [float(block[0]), float(block[1]), float(block[2]), float(block[3])]
                block_geom = box(*bbox)
                block_area = block_geom.area
                width = bbox[2] - bbox[0]
                height = bbox[3] - bbox[1]
                if block_area < max(180, typical_text_h * typical_text_h * 0.9) or width < 20 or height < max(8, typical_text_h * 0.6):
                    debug_counter["small"] += 1
                    continue

                raw_text = (block[4] or "").strip().replace("\n", " ")
                if len(raw_text) < 6:
                    debug_counter["short_text"] += 1
                    continue
                compact = re.sub(r"[\W_]+", "", raw_text, flags=re.UNICODE)
                if len(compact) < 5:
                    debug_counter["low_content"] += 1
                    continue

                relaxed_top_body_env = False
                if body_env:
                    bx0, by0, bx1, by1 = body_env
                    x_pad = max(page_rect.width * 0.04, typical_text_h * 2.0)
                    y_pad_top = max(page_rect.height * 0.02, typical_text_h * 2.4)
                    y_pad_bottom = max(page_rect.height * 0.015, typical_text_h * 1.8)
                    outside_x = bbox[2] < (bx0 - x_pad) or bbox[0] > (bx1 + x_pad)
                    outside_y = bbox[3] < (by0 - y_pad_top) or bbox[1] > (by1 + y_pad_bottom)
                    relaxed_top_body_env = (
                        not outside_x
                        and bbox[1] <= page_rect.height * 0.18
                        and bbox[3] <= by0 + max(typical_text_h * 2.6, 32.0)
                    )
                    if outside_x or (outside_y and not relaxed_top_body_env):
                        debug_counter["outside_body_envelope"] += 1
                        continue

                coverage = 0.0
                if mask_union and block_area > 0:
                    coverage = block_geom.intersection(mask_union).area / block_area

                # 表格/目录/图片内部候选不补，避免幽灵文本回流。
                in_container = False
                center = self._bbox_center(bbox)
                for cont in page_containers:
                    inter_ratio = self._bbox_overlap_ratio(bbox, cont["bbox"])
                    center_inside = self._center_in_bbox(center, cont["bbox"])
                    if cont["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC"} and (inter_ratio >= 0.28 or center_inside):
                        in_container = True
                        break
                    if cont["label"] == "PICTURE" and (inter_ratio >= 0.72 or (center_inside and block_area < self._bbox_area(cont["bbox"]) * 0.6)):
                        in_container = True
                        break
                if in_container:
                    debug_counter["in_container"] += 1
                    continue

                probe_item = {"bbox": bbox, "text_preview": raw_text}
                if self._looks_like_margin_noise(probe_item, page_rect):
                    debug_counter["margin_noise"] += 1
                    continue
                if self._looks_like_running_header_candidate(raw_text, bbox, page_rect, page_no, typical_text_h):
                    debug_counter["running_header"] += 1
                    continue
                if self._overlaps_noise_zone(bbox, page_no):
                    debug_counter["noise_zone"] += 1
                    continue

                # 公式型内容不作为 TEXT 回补，交给 complex/formula 车间处理。
                eq_score = self._equation_signal_score(raw_text)
                sym_cnt = len(re.findall(r"[\=\+\-\*/\^_<>±×÷∑∏∫√∞∂∇≈≠≤≥\(\)\[\]\{\}|]", raw_text))
                alnum_cnt = len(re.findall(r"[A-Za-z0-9]", raw_text))
                sym_ratio = sym_cnt / max(1, alnum_cnt)
                line_like_candidate = height <= max(word_line_h * 2.4, 26.0)
                equation_dominant = (
                    eq_score >= 3
                    or sym_ratio >= 0.45
                    or (eq_score >= 2 and line_like_candidate and sym_ratio >= 0.08)
                )
                if equation_dominant:
                    debug_counter["equation_like_skip"] += 1
                    continue

                profile = pick_profile(bbox)
                if not profile:
                    debug_counter["out_of_column"] += 1
                    continue

                same_col_refs = [ref for ref in profile["items"] if overlap_x_ratio(bbox, ref["bbox"]) >= 0.32]
                if not same_col_refs:
                    debug_counter["weak_column_alignment"] += 1
                    continue

                support = neighbor_support(bbox, same_col_refs)
                upstream = [ref for ref in same_col_refs if ref["bbox"][3] <= bbox[1]]
                downstream = [ref for ref in same_col_refs if ref["bbox"][1] >= bbox[3]]

                nearest_down = min(downstream, key=lambda ref: ref["bbox"][1]) if downstream else None
                down_gap = (nearest_down["bbox"][1] - bbox[3]) if nearest_down else None
                align_down = overlap_x_ratio(bbox, nearest_down["bbox"]) if nearest_down else 0.0
                align_up = max((overlap_x_ratio(bbox, ref["bbox"]) for ref in upstream), default=0.0)
                near_column_head = bbox[1] <= profile["y0"] + typical_text_h * 7.0
                column_head_gap = (
                    not upstream
                    and nearest_down is not None
                    and near_column_head
                    and 0 <= down_gap <= typical_text_h * 7.5
                    and align_down >= 0.5
                    and width <= max(profile["median_width"] * 1.28, page_rect.width * 0.5)
                )

                nearest_up = max(upstream, key=lambda ref: ref["bbox"][3]) if upstream else None
                up_gap = (bbox[1] - nearest_up["bbox"][3]) if nearest_up else None
                near_column_tail = nearest_up is not None and bbox[1] >= max(profile["y0"] + typical_text_h * 4.0, nearest_up["bbox"][3])
                column_tail_gap = (
                    nearest_up is not None
                    and not downstream
                    and near_column_tail
                    and 0 <= up_gap <= typical_text_h * 4.8
                    and align_up >= 0.5
                    and width <= max(profile["median_width"] * 1.32, page_rect.width * 0.54)
                    and bbox[3] <= page_rect.height * 0.96
                )

                nearest_wide_restart = None
                for blocker in wide_restart_blocks:
                    if blocker["bbox"][3] > bbox[1]:
                        continue
                    if overlap_x_ratio(bbox, blocker["bbox"]) < 0.18 and blocker["bbox"][0] <= bbox[0] <= blocker["bbox"][2]:
                        continue
                    gap = bbox[1] - blocker["bbox"][3]
                    if gap < 0 or gap > typical_text_h * 9.5:
                        continue
                    if nearest_wide_restart is None or blocker["bbox"][3] > nearest_wide_restart["bbox"][3]:
                        nearest_wide_restart = blocker
                restart_after_wide_block = (
                    not upstream
                    and nearest_down is not None
                    and nearest_wide_restart is not None
                    and align_down >= 0.42
                    and 0 <= down_gap <= typical_text_h * 9.0
                    and width <= max(profile["median_width"] * 1.35, page_rect.width * 0.56)
                )
                nearest_blocker_below = None
                for blocker in page_items:
                    if blocker["label"] not in {"FORMULA", "COMPLEX_BLOCK", "PICTURE", "TABLE"}:
                        continue
                    gap = blocker["bbox"][1] - bbox[3]
                    if gap < 0 or gap > max(word_line_h * 7.0, 72.0):
                        continue
                    if overlap_x_ratio(bbox, blocker["bbox"]) < 0.45:
                        continue
                    if nearest_blocker_below is None or blocker["bbox"][1] < nearest_blocker_below["bbox"][1]:
                        nearest_blocker_below = blocker
                head_before_blocker = (
                    not upstream
                    and nearest_blocker_below is not None
                    and bbox[1] <= page_rect.height * 0.16
                    and width <= max(profile["median_width"] * 1.35, page_rect.width * 0.56)
                )
                nearest_paragraph_blocker = None
                for blocker in page_items:
                    if blocker["label"] not in {"FORMULA", "COMPLEX_BLOCK"}:
                        continue
                    gap = blocker["bbox"][1] - bbox[3]
                    if gap < 0 or gap > max(word_line_h * 6.0, 54.0):
                        continue
                    if overlap_x_ratio(bbox, blocker["bbox"]) < 0.42:
                        continue
                    if nearest_paragraph_blocker is None or blocker["bbox"][1] < nearest_paragraph_blocker["bbox"][1]:
                        nearest_paragraph_blocker = blocker
                paragraph_chain_gap = (
                    nearest_up is not None
                    and nearest_up["label"] in {"TEXT", "LIST_ITEM"}
                    and 0 <= up_gap <= max(word_line_h * 3.2, 34.0)
                    and align_up >= 0.6
                    and nearest_paragraph_blocker is not None
                    and width <= max(profile["median_width"] * 1.36, page_rect.width * 0.56)
                    and height <= max(typical_text_h * 5.8, 74.0)
                )
                synthetic_column_singleton = (
                    profile.get("hint_only")
                    and len(same_col_refs) == 1
                    and same_col_refs[0]["label"] == "BLOCK_HINT"
                    and overlap_x_ratio(bbox, same_col_refs[0]["bbox"]) >= 0.96
                    and width <= page_rect.width * 0.52
                    and bbox[1] < page_rect.height * 0.45
                )

                head_single_line = (column_head_gap or head_before_blocker) and height <= typical_text_h * 1.9
                tail_single_line = column_tail_gap and height <= typical_text_h * 1.9

                local_bridge = False
                if nearest_up and nearest_down:
                    local_bridge = (
                        0 <= up_gap <= typical_text_h * 2.8
                        and 0 <= down_gap <= typical_text_h * 2.8
                        and align_up >= 0.45
                        and align_down >= 0.45
                    )

                if not downstream and not (
                    column_tail_gap
                    or synthetic_column_singleton
                    or head_before_blocker
                    or paragraph_chain_gap
                ):
                    debug_counter["no_downstream_anchor"] += 1
                    continue

                accepted_gap_mode = None
                if restart_after_wide_block:
                    accepted_gap_mode = "restart_after_wide_block"
                elif head_before_blocker:
                    accepted_gap_mode = "head_before_blocker"
                elif paragraph_chain_gap:
                    accepted_gap_mode = "paragraph_chain_gap"
                elif synthetic_column_singleton:
                    accepted_gap_mode = "synthetic_column_singleton"
                elif column_head_gap:
                    accepted_gap_mode = "column_head_gap"
                elif column_tail_gap:
                    accepted_gap_mode = "column_tail_gap"
                elif local_bridge:
                    accepted_gap_mode = "local_bridge"

                if not accepted_gap_mode:
                    debug_counter["gap_rule_reject"] += 1
                    continue

                # 防止“已有大框内再补小子框”（作者两行被拆成两框等）。
                c_cx, c_cy = self._bbox_center(bbox)
                contained_in_large_text = False
                for eb in text_existing_bboxes:
                    ex0, ey0, ex1, ey1 = eb
                    if not (ex0 <= c_cx <= ex1 and ey0 <= c_cy <= ey1):
                        continue
                    ex_area = self._bbox_area(eb)
                    if ex_area <= block_area * 1.6:
                        continue
                    overlap = self._bbox_intersection_area(bbox, eb) / max(1.0, block_area)
                    if overlap >= 0.42:
                        contained_in_large_text = True
                        break
                if contained_in_large_text:
                    debug_counter["contained_in_large_text"] += 1
                    continue

                # 防止把多行作者/单位等密集区域“桥接”为一个新 text。
                bridge_hits = 0
                for eb in text_existing_bboxes:
                    inter = self._bbox_intersection_area(bbox, eb)
                    if inter / max(1.0, block_area) >= 0.1:
                        bridge_hits += 1
                if bridge_hits >= 2 and height < typical_text_h * 3.2:
                    debug_counter["bridge_existing_texts"] += 1
                    continue

                if coverage >= 0.58:
                    debug_counter["high_coverage_skip"] += 1
                    continue
                if accepted_gap_mode in {"local_bridge", "column_tail_gap", "paragraph_chain_gap"} and support < 1:
                    debug_counter["insufficient_support"] += 1
                    continue
                if accepted_gap_mode not in {
                    "column_head_gap",
                    "restart_after_wide_block",
                    "column_tail_gap",
                    "synthetic_column_singleton",
                    "head_before_blocker",
                    "paragraph_chain_gap",
                } and support < 2:
                    debug_counter["insufficient_support"] += 1
                    continue
                if coverage >= 0.35 and support == 0:
                    debug_counter["coverage_no_support"] += 1
                    continue

                # 与已有块重叠关系：防止 containment 类重复。
                max_cover_by_existing = 0.0
                for existing in page_existing_bboxes:
                    inter = self._bbox_intersection_area(bbox, existing)
                    if inter <= 0:
                        continue
                    cand_ratio = inter / max(1.0, block_area)
                    if cand_ratio > max_cover_by_existing:
                        max_cover_by_existing = cand_ratio
                if max_cover_by_existing >= 0.55:
                    debug_counter["covered_by_existing"] += 1
                    continue

                block_sig = self._normalize_signature(raw_text)
                if block_sig and len(block_sig) >= 10 and block_sig in existing_sigs:
                    debug_counter["signature_duplicate"] += 1
                    continue

                # 避免与现有块高 IoU 重复（包含关系之外的近似重框）。
                max_iou_with_existing = max(
                    (self._bbox_iou(bbox, existing) for existing in page_existing_bboxes),
                    default=0.0,
                )
                if max_iou_with_existing >= 0.62:
                    debug_counter["iou_existing"] += 1
                    continue

                # 避免新增块之间相互重叠重复。
                max_iou_with_new = max(
                    (self._bbox_iou(bbox, existing) for existing in page_new_bboxes),
                    default=0.0,
                )
                if max_iou_with_new >= 0.7:
                    debug_counter["iou_new"] += 1
                    continue

                new_item = {
                    "_uid": str(uuid.uuid4()),
                    "_origin_id": -1,
                    "id": -1,
                    "label": "TEXT",
                    "page": page_no,
                    "bbox": [round(c, 2) for c in bbox],
                    "text": raw_text,
                    "text_preview": raw_text[:80].strip(),
                }
                new_items.append(new_item)
                page_new_bboxes.append(bbox)
                page_existing_bboxes.append(bbox)
                text_existing_bboxes.append(bbox)
                profile["items"].append(new_item)
                if block_sig and len(block_sig) >= 10:
                    existing_sigs.add(block_sig)
                debug_counter["added"] += 1
                debug_counter[accepted_gap_mode] += 1
                if head_single_line:
                    debug_counter["head_single_line"] += 1
                if tail_single_line:
                    debug_counter["tail_single_line"] += 1

        self.items.extend(new_items)
        print(
            "    [Workshop 4] 定向补缺完成，"
            f"新增 {len(new_items)} 个文本块；"
            f"跳过统计: {dict(debug_counter)}"
        )

    def workshop_4_6_partial_block_restorer(self):
        """
        阶段2-B.6：部分覆盖 block 的 line 级回补
        目标场景：
        - PDF 原生 block 同时包含“已覆盖行 + 漏掉行”，导致 block 级补漏失败
        - complex 完成后，在其上/下方仍残留一整段未框选正文
        """
        new_items = []
        stats = defaultdict(int)

        for page_no in range(1, len(self.doc) + 1):
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_items = [it for it in self.items if it["page"] == page_no]
            if not page_items:
                continue

            page_masks = [box(*it["bbox"]) for it in page_items]
            mask_union = unary_union(page_masks) if page_masks else None
            body_env = self._build_body_envelope(page_items, page_rect)
            page_containers = [
                it for it in page_items if it["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC", "PICTURE"}
            ]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            if not column_profiles:
                stats["no_columns"] += 1
                continue

            text_like = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "COMPLEX_BLOCK"}]
            text_heights = [
                it["bbox"][3] - it["bbox"][1]
                for it in text_like
                if 6 <= (it["bbox"][3] - it["bbox"][1]) <= 120
            ]
            typical_text_h = median(text_heights) if text_heights else 12.0
            word_line_h = self._estimate_word_line_height(page)
            page_existing_bboxes = [it["bbox"] for it in page_items]
            text_existing_bboxes = [it["bbox"] for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"}]
            text_lines = self._extract_text_lines(page)

            def overlap_x_ratio(a, b):
                return self._bbox_overlap_x_ratio(a, b)

            def pick_profile(candidate_bbox):
                cx, _ = self._bbox_center(candidate_bbox)
                x_pad = max(page_rect.width * 0.03, typical_text_h * 2.0)
                matches = []
                for profile in column_profiles:
                    if cx < profile["x0"] - x_pad or cx > profile["x1"] + x_pad:
                        continue
                    cand_overlap = max(
                        (overlap_x_ratio(candidate_bbox, ref["bbox"]) for ref in profile["items"]),
                        default=0.0,
                    )
                    matches.append((cand_overlap, profile))
                if not matches:
                    return None
                matches.sort(key=lambda x: x[0], reverse=True)
                return matches[0][1]

            for block in page.get_text("blocks"):
                if len(block) < 7 or block[6] != 0:
                    continue

                bb = [float(block[0]), float(block[1]), float(block[2]), float(block[3])]
                b_area = self._bbox_area(bb)
                if b_area <= 0:
                    continue
                if mask_union:
                    coverage = box(*bb).intersection(mask_union).area / b_area
                else:
                    coverage = 0.0
                if coverage < 0.12 or coverage > 0.88:
                    continue

                raw_text = (block[4] or "").strip().replace("\n", " ")
                if len(raw_text) < 40:
                    continue
                profile = pick_profile(bb)
                if not profile:
                    continue

                candidate_lines = []
                for line in text_lines:
                    lb = line["bbox"]
                    l_area = self._bbox_area(lb)
                    if l_area <= 0:
                        continue
                    if box(*lb).intersection(box(*bb)).area / l_area < 0.6:
                        continue
                    line_text = line["text"]
                    compact = re.sub(r"[\W_]+", "", line_text, flags=re.UNICODE)
                    if len(compact) < 5:
                        continue
                    if self._looks_like_margin_noise({"bbox": lb, "text_preview": line_text}, page_rect):
                        continue
                    if any(
                        self._bbox_overlap_ratio(lb, cont["bbox"]) >= 0.25
                        or self._center_in_bbox(self._bbox_center(lb), cont["bbox"])
                        for cont in page_containers
                    ):
                        continue
                    if mask_union:
                        line_cov = box(*lb).intersection(mask_union).area / l_area
                        if line_cov >= 0.35:
                            continue
                    eq_score = self._equation_signal_score(line_text)
                    sym_cnt = len(re.findall(r"[\=\+\-\*/\^_<>±×÷∑∏∫√∞∂∇≈≠≤≥\(\)\[\]\{\}|]", line_text))
                    alnum_cnt = len(re.findall(r"[A-Za-z0-9]", line_text))
                    sym_ratio = sym_cnt / max(1, alnum_cnt)
                    if eq_score >= 3 or sym_ratio >= 0.28:
                        continue
                    if overlap_x_ratio(lb, [profile["x0"], lb[1], profile["x1"], lb[3]]) < 0.28:
                        continue
                    candidate_lines.append(line)

                if len(candidate_lines) < 2:
                    continue

                line_clusters = self._cluster_text_lines(
                    candidate_lines,
                    max_vgap=max(word_line_h * 1.25, 10.0),
                    x_tolerance=max(page_rect.width * 0.03, typical_text_h * 1.8),
                )

                for cluster in line_clusters:
                    if len(cluster) < 2:
                        continue
                    cluster_bbox = self._bbox_union([ln["bbox"] for ln in cluster])
                    cluster_area = self._bbox_area(cluster_bbox)
                    if cluster_area <= 0:
                        continue
                    if body_env:
                        bx0, by0, bx1, by1 = body_env
                        if (
                            cluster_bbox[0] < bx0 - page_rect.width * 0.04
                            or cluster_bbox[2] > bx1 + page_rect.width * 0.04
                            or cluster_bbox[1] < by0 - typical_text_h * 2.0
                            or cluster_bbox[3] > by1 + typical_text_h * 1.5
                        ):
                            continue
                    if mask_union:
                        cluster_cov = box(*cluster_bbox).intersection(mask_union).area / cluster_area
                        if cluster_cov >= 0.22:
                            continue
                    max_cover_by_existing = max(
                        (self._bbox_overlap_ratio(cluster_bbox, eb) for eb in page_existing_bboxes),
                        default=0.0,
                    )
                    if max_cover_by_existing >= 0.5:
                        continue
                    if any(self._bbox_iou(cluster_bbox, nb["bbox"]) >= 0.62 for nb in new_items if nb["page"] == page_no):
                        continue

                    nearby_above = False
                    nearby_below = False
                    for ref in page_items:
                        ref_bbox = ref["bbox"]
                        if overlap_x_ratio(cluster_bbox, ref_bbox) < 0.32:
                            continue
                        if 0 <= cluster_bbox[1] - ref_bbox[3] <= max(typical_text_h * 4.0, 38.0):
                            nearby_above = True
                        if 0 <= ref_bbox[1] - cluster_bbox[3] <= max(typical_text_h * 4.0, 38.0):
                            nearby_below = True
                        if nearby_above and nearby_below:
                            break

                    if not (nearby_above or nearby_below):
                        continue

                    joined_text = " ".join(ln["text"] for ln in cluster).strip()
                    if len(joined_text) < 40:
                        continue

                    new_item = {
                        "_uid": str(uuid.uuid4()),
                        "_origin_id": -1,
                        "id": -1,
                        "label": "TEXT",
                        "page": page_no,
                        "bbox": [round(c, 2) for c in cluster_bbox],
                        "text": joined_text,
                        "text_preview": joined_text[:80].strip(),
                    }
                    new_items.append(new_item)
                    page_existing_bboxes.append(cluster_bbox)
                    text_existing_bboxes.append(cluster_bbox)
                    profile["items"].append(new_item)
                    stats["added"] += 1

        self.items.extend(new_items)
        print(
            "    [Workshop 4.6] 部分覆盖 block 回补完成，"
            f"新增 {len(new_items)} 个文本块；"
            f"统计: {dict(stats)}"
        )

    def workshop_4_55_block_edge_restorer(self):
        """
        阶段2-B.55：block 头尾缺行恢复
        目标场景：
        - 一个 PDF block 大部分已覆盖，但头/尾仍漏了几行
        - 页尾/页首存在整块未覆盖正文，但与同页正文列布局强一致
        """
        new_items = []
        stats = defaultdict(int)

        for page_no in range(1, len(self.doc) + 1):
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_items = [it for it in self.items if it["page"] == page_no]
            if not page_items:
                continue

            page_masks = [box(*it["bbox"]) for it in page_items]
            mask_union = unary_union(page_masks) if page_masks else None
            body_env = self._build_body_envelope(page_items, page_rect)
            page_containers = [
                it for it in page_items if it["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC", "PICTURE"}
            ]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            word_line_h = self._estimate_word_line_height(page)
            text_heights = [
                it["bbox"][3] - it["bbox"][1]
                for it in page_items
                if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "COMPLEX_BLOCK"} and 6 <= (it["bbox"][3] - it["bbox"][1]) <= 120
            ]
            typical_text_h = median(text_heights) if text_heights else 12.0
            page_existing_bboxes = [it["bbox"] for it in page_items]
            text_like_refs = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER"}]

            for block in self._extract_text_blocks_with_lines(page):
                bb = block["bbox"]
                b_area = self._bbox_area(bb)
                if b_area <= 0:
                    continue
                block_cov = box(*bb).intersection(mask_union).area / b_area if mask_union else 0.0
                raw_text = block["text"]
                if not raw_text:
                    continue
                if self._looks_like_margin_noise({"bbox": bb, "text_preview": raw_text}, page_rect):
                    continue
                if body_env:
                    bx0, by0, bx1, by1 = body_env
                    body_fail = (
                        bb[0] < bx0 - page_rect.width * 0.04
                        or bb[2] > bx1 + page_rect.width * 0.04
                        or bb[1] < by0 - typical_text_h * 2.4
                        or bb[3] > by1 + typical_text_h * 2.0
                    )
                    if body_fail and block_cov < 0.14:
                        continue
                if any(
                    self._bbox_overlap_ratio(bb, cont["bbox"]) >= 0.2
                    or self._center_in_bbox(self._bbox_center(bb), cont["bbox"])
                    for cont in page_containers
                ):
                    continue
                profile_idx = self._profile_idx_for_bbox(bb, column_profiles, page_rect.width, word_line_h)
                if profile_idx is not None:
                    profile = column_profiles[profile_idx]
                    if self._bbox_overlap_x_ratio(bb, [profile["x0"], bb[1], profile["x1"], bb[3]]) < 0.22:
                        continue

                line_covs = []
                for line in block["lines"]:
                    lb = line["bbox"]
                    l_area = max(1.0, self._bbox_area(lb))
                    line_cov = box(*lb).intersection(mask_union).area / l_area if mask_union else 0.0
                    line_covs.append(line_cov)

                covered = [idx for idx, cov in enumerate(line_covs) if cov >= 0.35]
                if covered:
                    segments = []
                    head_idx = [idx for idx in range(0, covered[0]) if line_covs[idx] < 0.22]
                    tail_idx = [idx for idx in range(covered[-1] + 1, len(block["lines"])) if line_covs[idx] < 0.22]
                    if head_idx:
                        segments.extend((group, "head") for group in self._group_consecutive_indices(head_idx) if group[-1] == covered[0] - 1)
                    if tail_idx:
                        segments.extend((group, "tail") for group in self._group_consecutive_indices(tail_idx) if group[0] == covered[-1] + 1)
                else:
                    segments = [([idx for idx, cov in enumerate(line_covs) if cov < 0.22], "full")] if block_cov < 0.14 else []

                for seg_indices, seg_kind in segments:
                    seg_indices = [idx for idx in seg_indices if 0 <= idx < len(block["lines"])]
                    if not seg_indices:
                        continue
                    seg_lines = [block["lines"][idx] for idx in seg_indices]
                    seg_bbox = self._bbox_union([ln["bbox"] for ln in seg_lines])
                    seg_text = " ".join(ln["text"] for ln in seg_lines).strip()
                    compact = re.sub(r"[\W_]+", "", seg_text, flags=re.UNICODE)
                    if len(compact) < 12:
                        continue
                    if any(self._bbox_iou(seg_bbox, nb["bbox"]) >= 0.62 for nb in new_items if nb["page"] == page_no):
                        continue
                    if max((self._bbox_overlap_ratio(seg_bbox, eb) for eb in page_existing_bboxes), default=0.0) >= 0.45:
                        continue

                    if seg_kind == "full":
                        seg_eq_score = self._equation_signal_score(seg_text)
                        companion = False
                        for ref in text_like_refs:
                            overlap_y = max(0.0, min(seg_bbox[3], ref["bbox"][3]) - max(seg_bbox[1], ref["bbox"][1]))
                            overlap_y /= max(1.0, min(seg_bbox[3] - seg_bbox[1], ref["bbox"][3] - ref["bbox"][1]))
                            same_band = overlap_y >= 0.2 or abs(seg_bbox[1] - ref["bbox"][1]) <= max(typical_text_h * 3.0, 24.0)
                            if not same_band:
                                continue
                            companion = True
                            break
                        if len(self._normalize_text(seg_text)) < 100 and seg_eq_score < 1:
                            continue
                        if not companion and seg_bbox[3] < page_rect.height * 0.72:
                            continue
                    else:
                        if len(seg_lines) == 1 and len(compact) < 18:
                            continue

                    new_item = self._make_text_block(page_no, seg_bbox, seg_text)
                    new_items.append(new_item)
                    page_existing_bboxes.append(seg_bbox)
                    text_like_refs.append(new_item)
                    stats[f"added_{seg_kind}"] += 1

        self.items.extend(new_items)
        print(
            "    [Workshop 4.55] block 头尾缺行恢复完成，"
            f"新增 {len(new_items)} 个文本块；"
            f"统计: {dict(stats)}"
        )

    def workshop_4_65_formula_micro_gap_filler(self):
        """
        阶段2-B.65：公式邻域中的微小文本补漏
        目标场景：
        - 两个公式之间夹着一行很短的说明文字
        - complex 左右侧贴着一个极短连接词（如 and / where / which...）
        """
        new_items = []
        stats = defaultdict(int)

        for page_no in range(1, len(self.doc) + 1):
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_items = [it for it in self.items if it["page"] == page_no]
            if not page_items:
                continue

            page_masks = [box(*it["bbox"]) for it in page_items]
            mask_union = unary_union(page_masks) if page_masks else None
            body_env = self._build_body_envelope(page_items, page_rect)
            page_containers = [
                it for it in page_items if it["label"] in {"TABLE", "DOCUMENT_INDEX", "TOC", "PICTURE"}
            ]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            text_heights = [
                it["bbox"][3] - it["bbox"][1]
                for it in page_items
                if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "COMPLEX_BLOCK"} and 6 <= (it["bbox"][3] - it["bbox"][1]) <= 120
            ]
            typical_text_h = median(text_heights) if text_heights else 12.0
            word_line_h = self._estimate_word_line_height(page)
            page_existing_bboxes = [it["bbox"] for it in page_items]
            formula_refs = [
                it
                for it in page_items
                if it["label"] == "FORMULA" or self._is_formula_like_complex(it)
            ]
            if not formula_refs:
                continue

            candidate_lines = []
            for line in self._extract_text_lines(page):
                lb = line["bbox"]
                l_area = self._bbox_area(lb)
                if l_area <= 0:
                    continue
                compact = re.sub(r"[\W_]+", "", line["text"], flags=re.UNICODE)
                if len(compact) < 3:
                    continue
                if self._looks_like_margin_noise({"bbox": lb, "text_preview": line["text"]}, page_rect):
                    continue
                if any(
                    self._bbox_overlap_ratio(lb, cont["bbox"]) >= 0.25
                    or self._center_in_bbox(self._bbox_center(lb), cont["bbox"])
                    for cont in page_containers
                ):
                    continue
                if mask_union:
                    line_cov = box(*lb).intersection(mask_union).area / l_area
                    if line_cov >= 0.18:
                        continue
                if body_env:
                    bx0, by0, bx1, by1 = body_env
                    if (
                        lb[0] < bx0 - page_rect.width * 0.04
                        or lb[2] > bx1 + page_rect.width * 0.04
                        or lb[1] < by0 - typical_text_h * 2.2
                        or lb[3] > by1 + typical_text_h * 1.8
                    ):
                        continue

                line_profile_idx = self._profile_idx_for_bbox(lb, column_profiles, page_rect.width, word_line_h)
                eq_score = self._equation_signal_score(line["text"])
                sym_cnt = len(re.findall(r"[\=\+\-\*/\^_<>±×÷∑∏∫√∞∂∇≈≠≤≥\(\)\[\]\{\}|]", line["text"]))
                alnum_cnt = len(re.findall(r"[A-Za-z0-9]", line["text"]))
                sym_ratio = sym_cnt / max(1, alnum_cnt)
                if eq_score >= 2 or sym_ratio >= 0.32:
                    continue

                near_above = False
                near_below = False
                sidecar = False
                for ref in formula_refs:
                    ref_profile_idx = self._profile_idx_for_bbox(ref["bbox"], column_profiles, page_rect.width, word_line_h)
                    if (
                        line_profile_idx is not None
                        and ref_profile_idx is not None
                        and line_profile_idx != ref_profile_idx
                    ):
                        line_profile = column_profiles[line_profile_idx]
                        ref_profile = column_profiles[ref_profile_idx]
                        stable_profile_mismatch = (
                            len(line_profile["items"]) >= 2
                            and len(ref_profile["items"]) >= 2
                            and line_profile["median_width"] >= page_rect.width * 0.25
                            and ref_profile["median_width"] >= page_rect.width * 0.25
                        )
                        if stable_profile_mismatch:
                            continue
                    overlap_x = self._bbox_overlap_x_ratio(lb, ref["bbox"])
                    h_gap = max(0.0, max(lb[0], ref["bbox"][0]) - min(lb[2], ref["bbox"][2]))
                    if overlap_x >= 0.22 or h_gap <= max(page_rect.width * 0.16, word_line_h * 8.0):
                        if 0 <= lb[1] - ref["bbox"][3] <= max(word_line_h * 3.8, 36.0):
                            near_above = True
                        if 0 <= ref["bbox"][1] - lb[3] <= max(word_line_h * 3.8, 36.0):
                            near_below = True
                    vertical_overlap = max(0.0, min(lb[3], ref["bbox"][3]) - max(lb[1], ref["bbox"][1]))
                    vertical_ratio = vertical_overlap / max(1.0, min(lb[3] - lb[1], ref["bbox"][3] - ref["bbox"][1]))
                    if vertical_ratio >= 0.55 and h_gap <= max(word_line_h * 4.0, 38.0):
                        sidecar = True

                line_w = lb[2] - lb[0]
                if not (
                    (near_above and near_below)
                    or (
                        sidecar
                        and line_w <= max(page_rect.width * 0.16, typical_text_h * 8.0)
                    )
                ):
                    continue

                max_cover_by_existing = max(
                    (self._bbox_overlap_ratio(lb, eb) for eb in page_existing_bboxes),
                    default=0.0,
                )
                if max_cover_by_existing >= 0.42:
                    continue
                candidate_lines.append(line)

            if not candidate_lines:
                continue

            line_clusters = self._cluster_text_lines(
                candidate_lines,
                max_vgap=max(word_line_h * 1.4, 11.0),
                x_tolerance=max(page_rect.width * 0.025, typical_text_h * 1.5),
            )
            for cluster in line_clusters:
                cluster_bbox = self._bbox_union([ln["bbox"] for ln in cluster])
                joined_text = " ".join(ln["text"] for ln in cluster).strip()
                compact = re.sub(r"[\W_]+", "", joined_text, flags=re.UNICODE)
                if len(cluster) < 2 and len(compact) < 3:
                    continue
                if any(self._bbox_iou(cluster_bbox, nb["bbox"]) >= 0.62 for nb in new_items if nb["page"] == page_no):
                    continue

                joined_norm = self._normalize_text(joined_text)
                for ref in page_items:
                    if ref["label"] not in {"TEXT", "LIST_ITEM"}:
                        continue
                    if self._bbox_intersection_area(cluster_bbox, ref["bbox"]) > 0:
                        continue
                    vertical_gap = max(0.0, cluster_bbox[1] - ref["bbox"][3])
                    if vertical_gap > max(word_line_h * 3.8, 36.0):
                        continue
                    ref_text = self._item_text(ref)
                    ref_norm = self._normalize_text(ref_text)
                    if not joined_norm or not ref_norm.endswith(joined_norm):
                        continue
                    trimmed_text = ref_text[: max(0, len(ref_text.rstrip()) - len(joined_text))].rstrip(" ,;:")
                    if len(self._normalize_text(trimmed_text)) < 6:
                        continue
                    ref["text"] = trimmed_text
                    ref["text_preview"] = trimmed_text[:80].strip()
                    stats["trimmed_suffix"] += 1
                    break

                new_item = self._make_text_block(page_no, cluster_bbox, joined_text)
                new_items.append(new_item)
                page_existing_bboxes.append(cluster_bbox)
                stats["added"] += 1

        self.items.extend(new_items)
        print(
            "    [Workshop 4.65] 公式微小文本补漏完成，"
            f"新增 {len(new_items)} 个文本块；"
            f"统计: {dict(stats)}"
        )

    def workshop_4_66_equation_line_restorer(self):
        """
        阶段2-B.66：孤立公式文本行补漏
        目标场景：
        - 一行短公式以文本方式存在，但 docling 和复杂块都未覆盖
        - 常见于“Consider the DG algebra ...”之后单独挂一行公式名
        """
        new_items = []
        stats = defaultdict(int)

        for page_no in range(1, len(self.doc) + 1):
            page = self.doc[page_no - 1]
            page_rect = page.rect
            page_items = [it for it in self.items if it["page"] == page_no]
            if not page_items:
                continue

            page_masks = [box(*it["bbox"]) for it in page_items]
            mask_union = unary_union(page_masks) if page_masks else None
            body_env = self._build_body_envelope(page_items, page_rect)
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            word_line_h = self._estimate_word_line_height(page)
            page_existing_bboxes = [it["bbox"] for it in page_items]
            text_refs = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER"}]

            for line in self._extract_text_lines(page):
                lb = line["bbox"]
                l_area = self._bbox_area(lb)
                if l_area <= 0:
                    continue
                if mask_union:
                    line_cov = box(*lb).intersection(mask_union).area / l_area
                    if line_cov >= 0.18:
                        continue
                if self._looks_like_margin_noise({"bbox": lb, "text_preview": line["text"]}, page_rect):
                    continue
                if body_env:
                    bx0, by0, bx1, by1 = body_env
                    if (
                        lb[0] < bx0 - page_rect.width * 0.04
                        or lb[2] > bx1 + page_rect.width * 0.04
                        or lb[1] < by0 - word_line_h * 2.4
                        or lb[3] > by1 + max(word_line_h * 4.5, 48.0)
                    ):
                        continue
                if max((self._bbox_overlap_ratio(lb, eb) for eb in page_existing_bboxes), default=0.0) >= 0.42:
                    continue

                eq_score = self._equation_signal_score(line["text"])
                sym_cnt = len(re.findall(r"[\=\+\-\*/\^_<>±×÷∑∏∫√∞∂∇≈≠≤≥\(\)\[\]\{\}|⊂⊃↪→←]", line["text"]))
                alnum_cnt = len(re.findall(r"[A-Za-z0-9]", line["text"]))
                sym_ratio = sym_cnt / max(1, alnum_cnt)
                line_w = lb[2] - lb[0]
                if eq_score < 1 and sym_ratio < 0.14:
                    continue
                if line_w > page_rect.width * 0.36:
                    continue

                line_profile_idx = self._profile_idx_for_bbox(lb, column_profiles, page_rect.width, word_line_h)
                near_above_text = False
                for ref in text_refs:
                    ref_profile_idx = self._profile_idx_for_bbox(ref["bbox"], column_profiles, page_rect.width, word_line_h)
                    if line_profile_idx is not None and ref_profile_idx is not None and line_profile_idx != ref_profile_idx:
                        continue
                    if self._bbox_overlap_x_ratio(lb, ref["bbox"]) < 0.28:
                        continue
                    if 0 <= lb[1] - ref["bbox"][3] <= max(word_line_h * 3.2, 30.0):
                        near_above_text = True
                        break
                if not near_above_text:
                    continue

                new_item = self._make_text_block(page_no, lb, line["text"])
                new_items.append(new_item)
                page_existing_bboxes.append(lb)
                stats["added"] += 1

        self.items.extend(new_items)
        print(
            "    [Workshop 4.66] 孤立公式文本行补漏完成，"
            f"新增 {len(new_items)} 个文本块；"
            f"统计: {dict(stats)}"
        )

    def workshop_4_75_single_column_complex_sidecar_absorber(self):
        """
        阶段2-B.75：单栏 sidecar 文本并入公式 complex
        目标场景：
        - 单栏页面中，complex 旁边贴着一个极短 sidecar 文本（如 and）
        - 这类 sidecar 在语义上属于公式区域，应整体交给 complex
        """
        remove_uids = set()
        new_items = []
        stats = defaultdict(int)

        for page_no in range(1, len(self.doc) + 1):
            page = self.doc[page_no - 1]
            page_rect = page.rect
            word_line_h = self._estimate_word_line_height(page)
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in remove_uids]
            column_profiles = self._compute_column_profiles(page_items, page_rect)
            stable_multicol = len(column_profiles) >= 2 and sum(1 for p in column_profiles if p["median_width"] >= page_rect.width * 0.25) >= 2
            if stable_multicol:
                continue

            complexes = [it for it in page_items if self._is_formula_like_complex(it)]
            snippets = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"}]
            if not complexes or not snippets:
                continue

            for complex_item in complexes:
                cb = complex_item["bbox"]
                cluster = [complex_item]
                cluster_bbox = list(cb)
                for snippet in snippets:
                    if snippet["_uid"] == complex_item["_uid"] or snippet["_uid"] in remove_uids:
                        continue
                    text = self._normalize_text(self._item_text(snippet))
                    if len(text) > 24:
                        continue
                    sb = snippet["bbox"]
                    vertical_overlap = max(0.0, min(cb[3], sb[3]) - max(cb[1], sb[1]))
                    vertical_ratio = vertical_overlap / max(1.0, min(cb[3] - cb[1], sb[3] - sb[1]))
                    h_gap = max(0.0, max(cb[0], sb[0]) - min(cb[2], sb[2]))
                    if vertical_ratio < 0.55:
                        continue
                    if h_gap > max(word_line_h * 4.0, 40.0):
                        continue
                    cluster.append(snippet)
                    cluster_bbox = self._bbox_union([cluster_bbox, sb])

                if len(cluster) <= 1:
                    continue

                remove_uids.add(complex_item["_uid"])
                for member in cluster[1:]:
                    remove_uids.add(member["_uid"])
                new_items.append(self._make_complex_block(page_no, [m["bbox"] for m in cluster], complex_item.get("text") or "FORMULA_COMPLEX_ABSORB"))
                stats["absorbed"] += len(cluster) - 1

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 4.75] 单栏 sidecar 文本吸收完成，"
            f"吸收文本块 {stats['absorbed']} 个。"
        )

    def workshop_4_7_formula_overlap_text_splitter(self):
        """
        阶段2-B.7：formula-like complex 边缘轻微重叠文本裁切
        目标场景：
        - 一个大正文块仅有末尾一行与 complex 发生擦边重叠
        - 不能粗暴吞掉整段正文，也不能保留最终重叠
        """
        remove_uids = set()
        new_items = []
        stats = defaultdict(int)

        for page_no in range(1, len(self.doc) + 1):
            page = self.doc[page_no - 1]
            page_rect = page.rect
            word_line_h = self._estimate_word_line_height(page)
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in remove_uids]
            raw_texts = [
                it
                for it in page_items
                if it["label"] in {"TEXT", "LIST_ITEM"} and it.get("_origin_id", -1) > 0
            ]
            complexes = [it for it in page_items if self._is_formula_like_complex(it)]
            if not raw_texts or not complexes:
                continue

            page_lines = self._extract_text_lines(page)
            for text_item in raw_texts:
                if text_item["_uid"] in remove_uids:
                    continue
                tb = text_item["bbox"]
                t_area = max(1.0, self._bbox_area(tb))
                t_height = tb[3] - tb[1]
                if t_height <= max(word_line_h * 4.2, 44.0):
                    continue

                for complex_item in complexes:
                    cb = complex_item["bbox"]
                    inter = self._bbox_intersection_area(tb, cb)
                    if inter <= 1.0:
                        continue
                    overlap_by_text = inter / t_area
                    overlap_x = self._bbox_overlap_x_ratio(tb, cb)
                    edge_strip_conflict = self._is_formula_complex_edge_strip_overlap(cb, tb, word_line_h)
                    if overlap_by_text < 0.02:
                        continue
                    if overlap_by_text > 0.26 and not edge_strip_conflict:
                        continue
                    if overlap_by_text > 0.38:
                        continue
                    if overlap_x < 0.45:
                        continue

                    split_items = self._split_text_item_around_exclude_bbox(
                        text_item,
                        cb,
                        page_lines,
                        page_rect,
                        word_line_h,
                    )
                    if not split_items:
                        continue

                    remove_uids.add(text_item["_uid"])
                    new_items.extend(split_items)
                    stats["split_blocks"] += 1
                    stats["split_outputs"] += len(split_items)
                    break

        self.items = [it for it in self.items if it["_uid"] not in remove_uids]
        self.items.extend(new_items)
        print(
            "    [Workshop 4.7] formula-like complex 边缘文本裁切完成，"
            f"裁切原始文本块 {stats['split_blocks']} 个，"
            f"产出 {stats['split_outputs']} 个文本块。"
        )

    def workshop_4_8_overlap_guard(self):
        """
        阶段2-C：重叠收敛
        1) 保护原始 Docling 文本块（TEXT/LIST_ITEM）。
        2) complex 仅做冲突收敛，不再继续扩张，避免跨栏误并。
        3) 最终保证 complex 不与其它块残留重叠。
        """
        pages = sorted(set(it["page"] for it in self.items))
        removed = set()
        complex_resolve_count = 0
        complex_drop_count = 0
        text_dedupe_count = 0

        for page_no in pages:
            page_rect = self.doc[page_no - 1].rect
            page_w = page_rect.width
            word_line_h = self._estimate_word_line_height(self.doc[page_no - 1])
            changed = True
            while changed:
                changed = False
                page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in removed]
                complexes = [it for it in page_items if it["label"] == "COMPLEX_BLOCK"]
                others = [
                    it
                    for it in page_items
                    if it["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER", "FORMULA", "COMPLEX_BLOCK"}
                ]
                for c in complexes:
                    if c["_uid"] in removed:
                        continue
                    cb = c["bbox"]
                    c_area = max(1.0, self._bbox_area(cb))
                    c_removed = False
                    for o in others:
                        if o["_uid"] == c["_uid"] or o["_uid"] in removed:
                            continue
                        ob = o["bbox"]
                        inter = self._bbox_intersection_area(cb, ob)
                        if inter <= 1.0:
                            continue
                        o_area = max(1.0, self._bbox_area(ob))
                        overlap_min = inter / min(c_area, o_area)
                        overlap_by_other = inter / o_area
                        overlap_x = self._bbox_overlap_x_ratio(cb, ob)
                        ox, oy = self._bbox_center(ob)
                        center_o_in_c = cb[0] <= ox <= cb[2] and cb[1] <= oy <= cb[3]
                        c_formula_like = self._is_formula_like_complex(c)

                        if o["label"] in {"TEXT", "LIST_ITEM", "SECTION_HEADER"}:
                            o_raw = o.get("_origin_id", -1) > 0 or o["label"] == "SECTION_HEADER"
                            if o_raw:
                                o_text = self._item_text(o)
                                text_len = len(self._normalize_text(o_text))
                                raw_eq_like = self._equation_signal_score(o_text) >= 1
                                o_height = o["bbox"][3] - o["bbox"][1]
                                o_width = o["bbox"][2] - o["bbox"][0]
                                edge_strip_conflict = (
                                    c_formula_like
                                    and not raw_eq_like
                                    and text_len >= 80
                                    and self._is_formula_complex_edge_strip_overlap(cb, o["bbox"], word_line_h)
                                )
                                if edge_strip_conflict:
                                    continue
                                mostly_covered = overlap_by_other >= 0.82
                                short_snippet = (
                                    text_len <= 10
                                    and o_area <= c_area * 0.08
                                    and mostly_covered
                                )
                                strong_text_conflict = (
                                    center_o_in_c
                                    or overlap_min >= 0.18
                                    or overlap_by_other >= 0.28
                                )
                                suspicious_wide_complex = (
                                    (cb[2] - cb[0]) >= page_w * 0.44
                                    and o_width >= page_w * 0.3
                                    and overlap_x >= 0.35
                                    and overlap_min >= 0.08
                                )
                                if not strong_text_conflict and not suspicious_wide_complex:
                                    continue
                                complex_should_win = (
                                    mostly_covered
                                    and (
                                        o["label"] == "SECTION_HEADER"
                                        or short_snippet
                                        or (text_len <= 20 and o_area <= c_area * 0.16)
                                        or raw_eq_like
                                    )
                                )
                                if c_formula_like and (
                                    raw_eq_like
                                    or mostly_covered
                                    or (
                                        overlap_by_other >= 0.46
                                        and (
                                            text_len <= 120
                                            or o_height <= max(word_line_h * 4.6, 54.0)
                                        )
                                    )
                                    or (
                                        o_height <= max(word_line_h * 2.8, 34.0)
                                        and overlap_x >= 0.55
                                        and overlap_by_other >= 0.14
                                    )
                                ):
                                    complex_should_win = True
                                if complex_should_win:
                                    removed.add(o["_uid"])
                                    text_dedupe_count += 1
                                    changed = True
                                else:
                                    # 原始文本优先：发生冲突时丢弃 complex，避免误伤正文。
                                    removed.add(c["_uid"])
                                    complex_drop_count += 1
                                    changed = True
                                    c_removed = True
                                    break
                            else:
                                if overlap_min >= 0.32 or (inter / o_area) >= 0.55 or center_o_in_c:
                                    removed.add(o["_uid"])
                                    complex_resolve_count += 1
                                    changed = True
                            continue

                        if o["label"] == "FORMULA":
                            if overlap_min >= 0.22 or center_o_in_c:
                                removed.add(o["_uid"])
                                complex_resolve_count += 1
                                changed = True
                            elif (inter / c_area) >= 0.36:
                                removed.add(c["_uid"])
                                complex_drop_count += 1
                                changed = True
                                c_removed = True
                                break
                            continue

                        if o["label"] == "COMPLEX_BLOCK":
                            cx, cy = self._bbox_center(cb)
                            center_c_in_o = ob[0] <= cx <= ob[2] and ob[1] <= cy <= ob[3]
                            if overlap_min >= 0.3 or center_o_in_c or center_c_in_o:
                                if o_area < c_area:
                                    loser = o
                                elif o_area > c_area:
                                    loser = c
                                else:
                                    loser = o if o["_uid"] > c["_uid"] else c
                                removed.add(loser["_uid"])
                                complex_resolve_count += 1
                                changed = True
                                if loser["_uid"] == c["_uid"]:
                                    c_removed = True
                                    break

                    if c_removed:
                        continue

            # 硬收敛：complex 与原始 text/list 仍重叠时，删除 complex。
            page_items = [it for it in self.items if it["page"] == page_no and it["_uid"] not in removed]
            complexes = [it for it in page_items if it["label"] == "COMPLEX_BLOCK"]
            raw_texts = [
                it
                for it in page_items
                if (
                    it["label"] in {"TEXT", "LIST_ITEM"} and it.get("_origin_id", -1) > 0
                ) or it["label"] == "SECTION_HEADER"
            ]
            for c in complexes:
                cb = c["bbox"]
                c_area = max(1.0, self._bbox_area(cb))
                c_formula_like = self._is_formula_like_complex(c)
                for t in raw_texts:
                    inter = self._bbox_intersection_area(cb, t["bbox"])
                    if inter <= 1.0:
                        continue
                    t_area = max(1.0, self._bbox_area(t["bbox"]))
                    text_len = len(self._normalize_text(self._item_text(t)))
                    raw_eq_like = self._equation_signal_score(self._item_text(t)) >= 1
                    overlap_min = inter / min(c_area, t_area)
                    overlap_by_text = inter / t_area
                    overlap_x = self._bbox_overlap_x_ratio(cb, t["bbox"])
                    t_height = t["bbox"][3] - t["bbox"][1]
                    if (
                        c_formula_like
                        and not raw_eq_like
                        and text_len >= 80
                        and self._is_formula_complex_edge_strip_overlap(cb, t["bbox"], word_line_h)
                    ):
                        continue
                    if c_formula_like and (
                        raw_eq_like
                        or overlap_by_text >= 0.82
                        or (
                            overlap_by_text >= 0.42
                            and (
                                text_len <= 120
                                or t_height <= max(word_line_h * 4.6, 54.0)
                            )
                        )
                        or self._formula_complex_text_overlap_significant(
                            overlap_min,
                            overlap_by_text,
                            overlap_x,
                            t_height,
                            word_line_h,
                        )
                    ):
                        removed.add(t["_uid"])
                        text_dedupe_count += 1
                        continue
                    if overlap_by_text >= 0.86 and (text_len <= 20 or raw_eq_like or t_area <= c_area * 0.16):
                        removed.add(t["_uid"])
                        text_dedupe_count += 1
                        continue
                    if overlap_min >= 0.22:
                        removed.add(c["_uid"])
                        complex_drop_count += 1
                        break

            # 文本去重：TEXT/LIST_ITEM 重叠时，优先保留原始 docling 块。
            texts = [it for it in page_items if it["label"] in {"TEXT", "LIST_ITEM"} and it["_uid"] not in removed]
            for i in range(len(texts)):
                a = texts[i]
                if a["_uid"] in removed:
                    continue
                for j in range(i + 1, len(texts)):
                    b = texts[j]
                    if b["_uid"] in removed:
                        continue
                    inter = self._bbox_intersection_area(a["bbox"], b["bbox"])
                    if inter <= 2.0:
                        continue
                    a_area = max(1.0, self._bbox_area(a["bbox"]))
                    b_area = max(1.0, self._bbox_area(b["bbox"]))
                    ov = inter / min(a_area, b_area)
                    if ov < 0.34:
                        continue

                    # 保留规则：原始 docling > 新增；若同源则保留面积更大者。
                    a_raw = a.get("_origin_id", -1) > 0
                    b_raw = b.get("_origin_id", -1) > 0
                    if a_raw and b_raw:
                        # 原始块之间只在“明显包含/重叠重复”时才去重，避免误删作者单位等密集区。
                        ax, ay = self._bbox_center(a["bbox"])
                        bx, by = self._bbox_center(b["bbox"])
                        a_in_b = b["bbox"][0] <= ax <= b["bbox"][2] and b["bbox"][1] <= ay <= b["bbox"][3]
                        b_in_a = a["bbox"][0] <= bx <= a["bbox"][2] and a["bbox"][1] <= by <= a["bbox"][3]
                        bigger = max(a_area, b_area)
                        smaller = min(a_area, b_area)
                        if ov >= 0.8 or ((a_in_b or b_in_a) and smaller <= bigger * 0.55):
                            loser = a if a_area < b_area else b
                        else:
                            continue
                    elif a_raw and not b_raw:
                        loser = b
                    elif b_raw and not a_raw:
                        loser = a
                    else:
                        loser = a if a_area < b_area else b
                    removed.add(loser["_uid"])
                    text_dedupe_count += 1

        self.items = [it for it in self.items if it["_uid"] not in removed]
        print(
            "    [Workshop 4.8] 重叠收敛完成："
            f"complex 冲突处理 {complex_resolve_count} 次，"
            f"complex 回退删除 {complex_drop_count} 个，"
            f"text 去重 {text_dedupe_count} 次。"
        )

    def workshop_5_safe_sorter(self):
        """
        排序策略：
        - 完整保留 Docling 原始块的相对顺序；
        - 双栏页按原始列序（left-first/right-first）插入新增块；
        - 其他页按几何上下文插入到相邻原始块之间；
        - 最终统一重编号为严格递增整数。
        """
        pages = sorted(set(it["page"] for it in self.items))
        final_sorted = []

        for page_no in pages:
            page_items = [it for it in self.items if it["page"] == page_no]
            originals = [it for it in page_items if it.get("_origin_id", -1) > 0]
            additions = [it for it in page_items if it.get("_origin_id", -1) <= 0]

            originals.sort(key=lambda x: x["_origin_id"])
            if not originals:
                additions.sort(key=lambda x: (x["bbox"][1], x["bbox"][0]))
                final_sorted.extend(additions)
                continue

            page_w = self.doc[page_no - 1].rect.width
            text_like_labels = {"TEXT", "LIST_ITEM", "COMPLEX_BLOCK", "FORMULA", "SECTION_HEADER", "TITLE"}
            text_like_originals = [it for it in originals if it["label"] in text_like_labels]
            col_like_originals = [it for it in text_like_originals if (it["bbox"][2] - it["bbox"][0]) < page_w * 0.75]
            two_col_mode = False
            left_first = True
            mid_x = page_w / 2.0
            left_idxs = []
            right_idxs = []
            idx_map = {id(it): idx for idx, it in enumerate(originals)}

            if len(col_like_originals) >= 4:
                centers = [((it["bbox"][0] + it["bbox"][2]) / 2.0) for it in col_like_originals]
                c_min, c_max = min(centers), max(centers)
                if c_max - c_min >= page_w * 0.28:
                    mid_x = (c_min + c_max) / 2.0
            for it in col_like_originals:
                idx = idx_map[id(it)]
                cx = (it["bbox"][0] + it["bbox"][2]) / 2.0
                if cx < mid_x:
                    left_idxs.append(idx)
                else:
                    right_idxs.append(idx)
            if len(left_idxs) >= 2 and len(right_idxs) >= 2:
                two_col_mode = True
                left_first = (sum(left_idxs) / len(left_idxs)) <= (sum(right_idxs) / len(right_idxs))
            left_dominant_mode = len(left_idxs) >= 3 and len(right_idxs) == 0
            right_dominant_mode = len(right_idxs) >= 3 and len(left_idxs) == 0

            positioned = []

            for idx, it in enumerate(originals):
                positioned.append((float(idx), 0, idx, it))

            def center(b):
                return ((b[0] + b[2]) / 2.0, (b[1] + b[3]) / 2.0)

            original_centers = []
            for idx, it in enumerate(originals):
                cx, cy = center(it["bbox"])
                original_centers.append((idx, cx, cy, it["bbox"]))

            additions_sorted = sorted(additions, key=lambda x: (x["bbox"][1], x["bbox"][0]))
            for add_idx, add in enumerate(additions_sorted):
                ax, ay = center(add["bbox"])
                aw = max(1.0, add["bbox"][2] - add["bbox"][0])

                if two_col_mode:
                    add_is_left = ax < mid_x
                    if left_first:
                        target_idxs = left_idxs if add_is_left else right_idxs
                        fallback_before = min(right_idxs) if (not add_is_left and right_idxs) else None
                        fallback_after = max(left_idxs) if (add_is_left and left_idxs) else None
                    else:
                        target_idxs = right_idxs if not add_is_left else left_idxs
                        fallback_before = min(left_idxs) if (add_is_left and left_idxs) else None
                        fallback_after = max(right_idxs) if (not add_is_left and right_idxs) else None

                    if target_idxs:
                        prevs = []
                        nexts = []
                        for idx in target_idxs:
                            ob = originals[idx]["bbox"]
                            oy = (ob[1] + ob[3]) / 2.0
                            if oy <= ay:
                                prevs.append((idx, oy))
                            else:
                                nexts.append((idx, oy))

                        if prevs and nexts:
                            pidx = max(prevs, key=lambda x: x[1])[0]
                            nidx = min(nexts, key=lambda x: x[1])[0]
                            insert_pos = (pidx + nidx) / 2.0 if nidx > pidx else pidx + 0.5
                        elif prevs:
                            pidx = max(prevs, key=lambda x: x[1])[0]
                            insert_pos = pidx + 0.5
                        elif nexts:
                            nidx = min(nexts, key=lambda x: x[1])[0]
                            insert_pos = nidx - 0.5
                        elif fallback_before is not None:
                            insert_pos = fallback_before - 0.5
                        elif fallback_after is not None:
                            insert_pos = fallback_after + 0.5
                        else:
                            insert_pos = len(originals) - 0.5
                    elif fallback_before is not None:
                        insert_pos = fallback_before - 0.5
                    elif fallback_after is not None:
                        insert_pos = fallback_after + 0.5
                    else:
                        insert_pos = len(originals) - 0.5

                    insert_pos += add_idx * 1e-4
                    positioned.append((insert_pos, 1, add_idx, add))
                    continue

                # 右栏缺失特判：原始块只有左栏连续流时，右栏新增块放在左栏流之后。
                if left_dominant_mode and ax > mid_x:
                    insert_pos = (max(left_idxs) + 0.5) if left_idxs else (len(originals) - 0.5)
                    insert_pos += add_idx * 1e-4
                    positioned.append((insert_pos, 1, add_idx, add))
                    continue
                if right_dominant_mode and ax < mid_x:
                    insert_pos = (max(right_idxs) + 0.5) if right_idxs else (len(originals) - 0.5)
                    insert_pos += add_idx * 1e-4
                    positioned.append((insert_pos, 1, add_idx, add))
                    continue

                related = []
                for idx, ocx, ocy, ob in original_centers:
                    ow = max(1.0, ob[2] - ob[0])
                    overlap_x = max(0.0, min(add["bbox"][2], ob[2]) - max(add["bbox"][0], ob[0]))
                    overlap_ratio = overlap_x / min(aw, ow)
                    if overlap_ratio >= 0.22 or abs(ocx - ax) <= page_w * 0.18:
                        related.append((idx, ocx, ocy))

                base = related if related else [(idx, ocx, ocy) for idx, ocx, ocy, _ in original_centers]
                prevs = [p for p in base if p[2] <= ay]
                nexts = [p for p in base if p[2] > ay]

                if prevs and nexts:
                    prev_idx = max(prevs, key=lambda x: x[2])[0]
                    next_idx = min(nexts, key=lambda x: x[2])[0]
                    if next_idx <= prev_idx:
                        insert_pos = prev_idx + 0.5
                    else:
                        insert_pos = (prev_idx + next_idx) / 2.0
                elif prevs:
                    prev_idx = max(prevs, key=lambda x: x[2])[0]
                    insert_pos = prev_idx + 0.5
                elif nexts:
                    next_idx = min(nexts, key=lambda x: x[2])[0]
                    insert_pos = next_idx - 0.5
                else:
                    insert_pos = len(originals) - 0.5

                # 用微小偏移稳定同一插槽内新增块的相对顺序。
                insert_pos += add_idx * 1e-4
                positioned.append((insert_pos, 1, add_idx, add))

            positioned.sort(key=lambda x: (x[0], x[1], x[2]))
            final_sorted.extend([it for _, _, _, it in positioned])

        for idx, item in enumerate(final_sorted, start=1):
            item["id"] = idx
            item.pop("_uid", None)
            item.pop("_origin_id", None)

        self.items = final_sorted
        print(f"    [Workshop 5] 安全排序完成，最终保留 {len(self.items)} 个元素。")

    def save(self, output_path):
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.items, f, indent=2, ensure_ascii=False)
        self.doc.close()


def run_patch_engine(pdf_path, raw_json_path, final_json_path):
    engine = PatchEngine(pdf_path, raw_json_path)
    engine.workshop_1_scavenger()
    engine.workshop_2_margin_noise_filter()
    engine.workshop_3_formula_text_resolver()
    engine.workshop_3_5_formula_complexifier()
    engine.workshop_3_45_solitary_formula_anchor_fallback()
    engine.workshop_3_55_formula_stack_merger()
    engine.workshop_3_6_complex_closure()
    engine.workshop_3_7_formula_complex_absorber()
    engine.workshop_3_75_formula_complex_drawing_extender()
    engine.workshop_4_targeted_gap_filler()
    engine.workshop_4_8_overlap_guard()
    engine.workshop_4_6_partial_block_restorer()
    engine.workshop_4_55_block_edge_restorer()
    engine.workshop_4_65_formula_micro_gap_filler()
    engine.workshop_4_66_equation_line_restorer()
    engine.workshop_4_75_single_column_complex_sidecar_absorber()
    engine.workshop_4_7_formula_overlap_text_splitter()
    engine.workshop_4_8_overlap_guard()
    engine.workshop_5_safe_sorter()
    engine.save(final_json_path)
