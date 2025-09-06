# module_12_entity_resolution.py
# Module 12: Entity Resolution - Identifying same entities across sources
# Fuzzy matching, deduplication, and master data management

import os
import json
import hashlib
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from collections import defaultdict
import difflib
import re
from enum import Enum

# Import LLM interface from earlier modules
from llm_providers import llm_interface, llm_json_call

# Part 1: Entity Resolution Fundamentals


class MatchType(Enum):
    # Types of entity matches
    EXACT = "exact"
    FUZZY = "fuzzy"
    PHONETIC = "phonetic"
    SEMANTIC = "semantic"
    PROBABILISTIC = "probabilistic"
    NO_MATCH = "no_match"


@dataclass
class EntityRecord:
    # Represents an entity from any source
    source_id: str
    record_id: str
    attributes: Dict[str, Any]
    confidence: float = 1.0

    def get_blocking_key(self, attributes: List[str]) -> str:
        # Generate blocking key for candidate selection
        key_parts = []
        for attr in attributes:
            value = self.attributes.get(attr, "")
            if value:
                # Normalize and take first few characters
                normalized = str(value).lower().strip()
                key_parts.append(normalized[:3])

        return "_".join(key_parts) if key_parts else "unknown"

    def to_comparison_vector(self, attributes: List[str]) -> List[Any]:
        # Convert to vector for comparison
        return [self.attributes.get(attr) for attr in attributes]


@dataclass
class MatchCandidate:
    # A potential match between two entities
    entity1: EntityRecord
    entity2: EntityRecord
    match_score: float
    match_type: MatchType
    attribute_scores: Dict[str, float]

    @property
    def is_match(self) -> bool:
        return self.match_score > 0.8 and self.match_type != MatchType.NO_MATCH


class SimilarityCalculator:
    # Calculate similarity between entity attributes

    def __init__(self):
        self.string_metrics = {
            'exact': self.exact_match,
            'levenshtein': self.levenshtein_similarity,
            'jaro_winkler': self.jaro_winkler_similarity,
            'token_sort': self.token_sort_similarity,
            'phonetic': self.phonetic_similarity
        }

    def exact_match(self, str1: str, str2: str) -> float:
        # Exact string match
        if str1 is None or str2 is None:
            return 0.0
        return 1.0 if str(str1).lower().strip() == str(str2).lower().strip() else 0.0

    def levenshtein_similarity(self, str1: str, str2: str) -> float:
        # Levenshtein distance normalized to similarity
        if str1 is None or str2 is None:
            return 0.0

        str1, str2 = str(str1).lower(), str(str2).lower()

        if len(str1) == 0 or len(str2) == 0:
            return 0.0

        # Calculate Levenshtein distance
        if len(str1) < len(str2):
            str1, str2 = str2, str1

        distances = range(len(str2) + 1)

        for i, char1 in enumerate(str1):
            new_distances = [i + 1]
            for j, char2 in enumerate(str2):
                if char1 == char2:
                    new_distances.append(distances[j])
                else:
                    new_distances.append(
                        1 + min(distances[j], distances[j + 1], new_distances[-1]))
            distances = new_distances

        # Convert to similarity
        max_len = max(len(str1), len(str2))
        return 1.0 - (distances[-1] / max_len)

    def jaro_winkler_similarity(self, str1: str, str2: str) -> float:
        # Jaro-Winkler similarity
        if str1 is None or str2 is None:
            return 0.0

        str1, str2 = str(str1).lower(), str(str2).lower()

        # Jaro similarity
        if str1 == str2:
            return 1.0

        len1, len2 = len(str1), len(str2)

        if len1 == 0 or len2 == 0:
            return 0.0

        match_window = max(len1, len2) // 2 - 1
        match_window = max(0, match_window)

        matches1 = [False] * len1
        matches2 = [False] * len2

        matches = 0
        transpositions = 0

        # Find matches
        for i in range(len1):
            start = max(0, i - match_window)
            end = min(i + match_window + 1, len2)

            for j in range(start, end):
                if matches2[j] or str1[i] != str2[j]:
                    continue
                matches1[i] = matches2[j] = True
                matches += 1
                break

        if matches == 0:
            return 0.0

        # Find transpositions
        k = 0
        for i in range(len1):
            if not matches1[i]:
                continue
            while not matches2[k]:
                k += 1
            if str1[i] != str2[k]:
                transpositions += 1
            k += 1

        jaro = (matches / len1 + matches / len2 +
                (matches - transpositions / 2) / matches) / 3.0

        # Jaro-Winkler adjustment
        prefix = 0
        for i in range(min(len1, len2)):
            if str1[i] == str2[i]:
                prefix += 1
            else:
                break

        prefix = min(4, prefix)

        return jaro + prefix * 0.1 * (1.0 - jaro)

    def token_sort_similarity(self, str1: str, str2: str) -> float:
        # Token-based similarity (good for names)
        if str1 is None or str2 is None:
            return 0.0

        # Tokenize and sort
        tokens1 = sorted(str(str1).lower().split())
        tokens2 = sorted(str(str2).lower().split())

        # Calculate Jaccard similarity
        set1, set2 = set(tokens1), set(tokens2)

        if not set1 and not set2:
            return 1.0
        if not set1 or not set2:
            return 0.0

        intersection = set1 & set2
        union = set1 | set2

        return len(intersection) / len(union)

    def phonetic_similarity(self, str1: str, str2: str) -> float:
        # Phonetic similarity using Soundex
        if str1 is None or str2 is None:
            return 0.0

        def soundex(name):
            # Simple Soundex implementation
            name = str(name).upper()
            soundex_mapping = {
                'BFPV': '1', 'CGJKQSXZ': '2', 'DT': '3',
                'L': '4', 'MN': '5', 'R': '6'
            }

            code = name[0] if name else ''

            for char in name[1:]:
                for key in soundex_mapping:
                    if char in key:
                        code += soundex_mapping[key]
                        break

            # Remove consecutive duplicates
            code = ''.join(c for i, c in enumerate(
                code) if i == 0 or c != code[i-1])

            # Pad with zeros
            code = (code + '0000')[:4]

            return code

        return 1.0 if soundex(str1) == soundex(str2) else 0.0

    def numeric_similarity(self, num1: Any, num2: Any, tolerance: float = 0.01) -> float:
        # Numeric similarity with tolerance
        try:
            val1, val2 = float(num1), float(num2)

            if val1 == val2:
                return 1.0

            # Relative difference
            max_val = max(abs(val1), abs(val2))
            if max_val == 0:
                return 1.0

            diff = abs(val1 - val2) / max_val

            if diff <= tolerance:
                return 1.0 - diff

            return 0.0

        except (TypeError, ValueError):
            return 0.0

    def date_similarity(self, date1: Any, date2: Any, max_days_diff: int = 30) -> float:
        # Date similarity based on difference
        try:
            # Parse dates (simplified - real implementation would handle more formats)
            if isinstance(date1, str):
                date1 = datetime.strptime(date1[:10], '%Y-%m-%d')
            if isinstance(date2, str):
                date2 = datetime.strptime(date2[:10], '%Y-%m-%d')

            days_diff = abs((date1 - date2).days)

            if days_diff == 0:
                return 1.0
            elif days_diff <= max_days_diff:
                return 1.0 - (days_diff / max_days_diff)
            else:
                return 0.0

        except:
            return 0.0

# Part 2: Blocking and Candidate Generation


class BlockingStrategy:
    # Reduce comparison space through blocking

    def __init__(self, blocking_attributes: List[str]):
        self.blocking_attributes = blocking_attributes
        self.blocks = defaultdict(list)

    def create_blocks(self, entities: List[EntityRecord]) -> Dict[str, List[EntityRecord]]:
        # Create blocks of potentially matching entities

        self.blocks.clear()

        for entity in entities:
            # Generate multiple blocking keys per entity
            blocking_keys = self.generate_blocking_keys(entity)

            for key in blocking_keys:
                self.blocks[key].append(entity)

        return self.blocks

    def generate_blocking_keys(self, entity: EntityRecord) -> Set[str]:
        # Generate multiple blocking keys for an entity

        keys = set()

        # Single attribute blocks
        for attr in self.blocking_attributes:
            if attr in entity.attributes:
                value = str(entity.attributes[attr]).lower().strip()

                # Exact first N characters
                if len(value) >= 3:
                    keys.add(f"{attr}_{value[:3]}")

                # Soundex for names
                if 'name' in attr.lower():
                    soundex_key = self.get_soundex(value)
                    keys.add(f"{attr}_soundex_{soundex_key}")

                # Numeric binning
                if isinstance(entity.attributes[attr], (int, float)):
                    bin_value = int(entity.attributes[attr] / 100) * 100
                    keys.add(f"{attr}_bin_{bin_value}")

        # Composite blocks
        if len(self.blocking_attributes) >= 2:
            composite_key = entity.get_blocking_key(
                self.blocking_attributes[:2])
            keys.add(f"composite_{composite_key}")

        return keys

    def get_soundex(self, value: str) -> str:
        # Simple Soundex for blocking
        if not value:
            return "0000"

        value = value.upper()
        soundex = value[0]

        mapping = {'BFPV': '1', 'CGJKQSXZ': '2',
                   'DT': '3', 'L': '4', 'MN': '5', 'R': '6'}

        for char in value[1:]:
            for key, code in mapping.items():
                if char in key:
                    if soundex[-1] != code:
                        soundex += code
                    break

        return (soundex + '0000')[:4]

    def get_candidate_pairs(self, entities: List[EntityRecord]) -> List[Tuple[EntityRecord, EntityRecord]]:
        # Get all candidate pairs from blocks

        blocks = self.create_blocks(entities)
        candidates = set()

        for block_key, block_entities in blocks.items():
            # All pairs within block
            for i in range(len(block_entities)):
                for j in range(i + 1, len(block_entities)):
                    # Avoid duplicate pairs
                    pair = tuple(
                        sorted([block_entities[i].record_id, block_entities[j].record_id]))
                    if pair not in candidates:
                        candidates.add(pair)
                        yield (block_entities[i], block_entities[j])

# Part 3: Machine Learning-based Matching


class MLEntityMatcher:
    # Machine learning approach to entity matching

    def __init__(self):
        self.similarity_calc = SimilarityCalculator()
        self.feature_weights = {
            'name_similarity': 0.3,
            'address_similarity': 0.2,
            'phone_similarity': 0.15,
            'email_similarity': 0.15,
            'id_similarity': 0.2
        }
        self.threshold = 0.75

    def extract_features(self, entity1: EntityRecord, entity2: EntityRecord) -> Dict[str, float]:
        # Extract similarity features for ML model

        features = {}

        # Name similarity (multiple metrics)
        if 'name' in entity1.attributes and 'name' in entity2.attributes:
            name1, name2 = entity1.attributes['name'], entity2.attributes['name']

            features['name_exact'] = self.similarity_calc.exact_match(
                name1, name2)
            features['name_levenshtein'] = self.similarity_calc.levenshtein_similarity(
                name1, name2)
            features['name_jaro'] = self.similarity_calc.jaro_winkler_similarity(
                name1, name2)
            features['name_token'] = self.similarity_calc.token_sort_similarity(
                name1, name2)
            features['name_phonetic'] = self.similarity_calc.phonetic_similarity(
                name1, name2)

            # Aggregate name similarity
            features['name_similarity'] = max(
                features['name_levenshtein'],
                features['name_jaro'],
                features['name_token']
            )
        else:
            features['name_similarity'] = 0.0

        # Address similarity
        if 'address' in entity1.attributes and 'address' in entity2.attributes:
            addr1, addr2 = entity1.attributes['address'], entity2.attributes['address']
            features['address_similarity'] = self.similarity_calc.token_sort_similarity(
                addr1, addr2)
        else:
            features['address_similarity'] = 0.0

        # Phone similarity
        if 'phone' in entity1.attributes and 'phone' in entity2.attributes:
            phone1 = re.sub(r'\D', '', str(entity1.attributes['phone']))
            phone2 = re.sub(r'\D', '', str(entity2.attributes['phone']))

            features['phone_similarity'] = self.similarity_calc.exact_match(
                phone1, phone2)
        else:
            features['phone_similarity'] = 0.0

        # Email similarity
        if 'email' in entity1.attributes and 'email' in entity2.attributes:
            email1, email2 = entity1.attributes['email'], entity2.attributes['email']
            features['email_similarity'] = self.similarity_calc.exact_match(
                email1, email2)
        else:
            features['email_similarity'] = 0.0

        # ID similarity
        if 'id' in entity1.attributes and 'id' in entity2.attributes:
            features['id_similarity'] = self.similarity_calc.exact_match(
                entity1.attributes['id'],
                entity2.attributes['id']
            )
        else:
            features['id_similarity'] = 0.0

        return features

    def calculate_match_score(self, features: Dict[str, float]) -> float:
        # Calculate weighted match score

        score = 0.0
        total_weight = 0.0

        for feature, weight in self.feature_weights.items():
            if feature in features:
                score += features[feature] * weight
                total_weight += weight

        return score / total_weight if total_weight > 0 else 0.0

    def predict_match(self, entity1: EntityRecord, entity2: EntityRecord) -> MatchCandidate:
        # Predict if two entities match

        features = self.extract_features(entity1, entity2)
        score = self.calculate_match_score(features)

        # Determine match type
        if score >= 0.95:
            match_type = MatchType.EXACT
        elif score >= self.threshold:
            match_type = MatchType.FUZZY
        else:
            match_type = MatchType.NO_MATCH

        return MatchCandidate(
            entity1=entity1,
            entity2=entity2,
            match_score=score,
            match_type=match_type,
            attribute_scores=features
        )

    def train_from_feedback(self, matches: List[MatchCandidate], labels: List[bool]):
        # Update weights based on human feedback

        # Simple gradient update (real implementation would use proper ML)
        learning_rate = 0.01

        for match, label in zip(matches, labels):
            predicted = match.match_score >= self.threshold
            error = int(label) - int(predicted)

            if error != 0:
                # Update weights
                for feature, value in match.attribute_scores.items():
                    if feature in self.feature_weights:
                        self.feature_weights[feature] += learning_rate * \
                            error * value

                # Normalize weights
                total = sum(self.feature_weights.values())
                for feature in self.feature_weights:
                    self.feature_weights[feature] /= total

# Part 4: Graph-based Entity Resolution


class EntityGraph:
    # Graph representation for entity resolution

    def __init__(self):
        self.nodes = {}  # entity_id -> EntityRecord
        self.edges = defaultdict(list)  # entity_id -> [(entity_id, weight)]
        self.clusters = []

    def add_entity(self, entity: EntityRecord):
        # Add entity as node
        self.nodes[entity.record_id] = entity

    def add_match(self, entity1_id: str, entity2_id: str, weight: float):
        # Add edge between matching entities
        if entity1_id in self.nodes and entity2_id in self.nodes:
            self.edges[entity1_id].append((entity2_id, weight))
            self.edges[entity2_id].append((entity1_id, weight))

    def find_connected_components(self, threshold: float = 0.7) -> List[Set[str]]:
        # Find connected components (entity clusters)

        visited = set()
        components = []

        for node_id in self.nodes:
            if node_id not in visited:
                component = set()
                self._dfs(node_id, visited, component, threshold)

                if component:
                    components.append(component)

        self.clusters = components
        return components

    def _dfs(self, node_id: str, visited: Set[str], component: Set[str], threshold: float):
        # Depth-first search for connected components

        visited.add(node_id)
        component.add(node_id)

        for neighbor_id, weight in self.edges[node_id]:
            if neighbor_id not in visited and weight >= threshold:
                self._dfs(neighbor_id, visited, component, threshold)

    def get_cluster_representative(self, cluster: Set[str]) -> EntityRecord:
        # Select best representative from cluster

        if not cluster:
            return None

        # Score each entity based on completeness and connections
        best_entity = None
        best_score = -1

        for entity_id in cluster:
            entity = self.nodes[entity_id]

            # Completeness score
            completeness = sum(
                1 for v in entity.attributes.values() if v) / len(entity.attributes)

            # Centrality score (number of connections)
            centrality = len([e for e, w in self.edges[entity_id] if w >= 0.7])

            score = completeness + centrality * 0.1

            if score > best_score:
                best_score = score
                best_entity = entity

        return best_entity

    def merge_entities(self, cluster: Set[str]) -> EntityRecord:
        # Merge multiple entities into single master record

        if not cluster:
            return None

        # Start with representative
        master = self.get_cluster_representative(cluster)

        if not master:
            return None

        # Merge attributes from all entities
        merged_attributes = master.attributes.copy()

        for entity_id in cluster:
            if entity_id == master.record_id:
                continue

            entity = self.nodes[entity_id]

            for attr, value in entity.attributes.items():
                if attr not in merged_attributes or not merged_attributes[attr]:
                    merged_attributes[attr] = value
                elif value and value != merged_attributes[attr]:
                    # Conflict resolution - keep longer/more complete value
                    if isinstance(value, str) and isinstance(merged_attributes[attr], str):
                        if len(str(value)) > len(str(merged_attributes[attr])):
                            merged_attributes[attr] = value

        # Create merged entity
        return EntityRecord(
            source_id="merged",
            record_id=f"master_{master.record_id}",
            attributes=merged_attributes,
            confidence=0.9
        )

# Part 5: Probabilistic Record Linkage


class ProbabilisticLinker:
    # Fellegi-Sunter probabilistic record linkage

    def __init__(self):
        self.m_probabilities = {}  # P(agreement | match)
        self.u_probabilities = {}  # P(agreement | non-match)
        self.lambda_threshold = 0.0  # Log-likelihood ratio threshold

    def estimate_probabilities(self, training_pairs: List[Tuple[EntityRecord, EntityRecord, bool]]):
        # Estimate m and u probabilities from training data

        # Count agreements for each attribute
        attribute_agreements = defaultdict(
            lambda: {'match': 0, 'non_match': 0})
        match_count = 0
        non_match_count = 0

        calc = SimilarityCalculator()

        for entity1, entity2, is_match in training_pairs:
            if is_match:
                match_count += 1
            else:
                non_match_count += 1

            # Check agreement for each attribute
            for attr in entity1.attributes:
                if attr in entity2.attributes:
                    sim = calc.exact_match(
                        entity1.attributes[attr], entity2.attributes[attr])

                    if sim > 0.9:  # Agreement
                        if is_match:
                            attribute_agreements[attr]['match'] += 1
                        else:
                            attribute_agreements[attr]['non_match'] += 1

        # Calculate probabilities
        for attr, counts in attribute_agreements.items():
            # m-probability: P(agreement | match)
            self.m_probabilities[attr] = counts['match'] / \
                match_count if match_count > 0 else 0.9

            # u-probability: P(agreement | non-match)
            self.u_probabilities[attr] = counts['non_match'] / \
                non_match_count if non_match_count > 0 else 0.1

        # Set threshold using EM algorithm (simplified)
        self.lambda_threshold = 0.0  # Log odds ratio threshold

    def calculate_match_weight(self, entity1: EntityRecord, entity2: EntityRecord) -> float:
        # Calculate Fellegi-Sunter match weight

        calc = SimilarityCalculator()
        log_weight = 0.0

        for attr in entity1.attributes:
            if attr in entity2.attributes:
                # Check agreement
                sim = calc.exact_match(
                    entity1.attributes[attr], entity2.attributes[attr])

                # Get probabilities
                m = self.m_probabilities.get(attr, 0.9)
                u = self.u_probabilities.get(attr, 0.1)

                if sim > 0.9:  # Agreement
                    if m > 0 and u > 0:
                        log_weight += np.log(m / u)
                else:  # Disagreement
                    if (1-m) > 0 and (1-u) > 0:
                        log_weight += np.log((1-m) / (1-u))

        return log_weight

    def classify_pair(self, entity1: EntityRecord, entity2: EntityRecord) -> MatchType:
        # Classify pair as match/non-match/uncertain

        weight = self.calculate_match_weight(entity1, entity2)

        if weight > self.lambda_threshold + 2:
            return MatchType.EXACT
        elif weight > self.lambda_threshold:
            return MatchType.PROBABILISTIC
        else:
            return MatchType.NO_MATCH

# Part 6: LLM-Enhanced Entity Resolution


class LLMEntityResolver:
    # Use LLM for complex entity resolution

    def __init__(self):
        self.cache = {}

    def resolve_with_llm(self, entity1: EntityRecord, entity2: EntityRecord) -> MatchCandidate:
        # Use LLM to determine if entities match

        # Check cache
        cache_key = f"{entity1.record_id}_{entity2.record_id}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Prepare prompt
        prompt = f"""
        Determine if these two records refer to the same entity:
        
        Record 1:
        {json.dumps(entity1.attributes, indent=2)}
        
        Record 2:
        {json.dumps(entity2.attributes, indent=2)}
        
        Return JSON with:
        - is_match: true/false
        - confidence: 0.0 to 1.0
        - reasoning: brief explanation
        - matching_fields: list of fields that match
        - conflicting_fields: list of fields that conflict
        
        JSON only:"""

        result = llm_json_call(prompt)

        if result:
            match_type = MatchType.SEMANTIC if result.get(
                'is_match') else MatchType.NO_MATCH

            candidate = MatchCandidate(
                entity1=entity1,
                entity2=entity2,
                match_score=result.get('confidence', 0.5),
                match_type=match_type,
                attribute_scores={
                    field: 1.0 for field in result.get('matching_fields', [])
                }
            )
        else:
            candidate = MatchCandidate(
                entity1=entity1,
                entity2=entity2,
                match_score=0.5,
                match_type=MatchType.NO_MATCH,
                attribute_scores={}
            )

        self.cache[cache_key] = candidate
        return candidate

    def resolve_conflicts(self, entities: List[EntityRecord]) -> EntityRecord:
        # Use LLM to resolve conflicts when merging

        prompt = f"""
        These records have been identified as the same entity but have conflicting values.
        Determine the most accurate merged record:
        
        Records:
        {json.dumps([e.attributes for e in entities], indent=2)}
        
        Return JSON with the best value for each field, explaining conflicts:
        - merged_record: final attribute values
        - conflict_resolutions: explanations for conflicting fields
        
        JSON only:"""

        result = llm_json_call(prompt)

        if result and 'merged_record' in result:
            return EntityRecord(
                source_id="llm_merged",
                record_id=f"merged_{entities[0].record_id}",
                attributes=result['merged_record'],
                confidence=0.85
            )

        # Fallback to first entity
        return entities[0]

# Part 7: Complete Entity Resolution Pipeline


class EntityResolutionPipeline:
    # Complete pipeline for entity resolution

    def __init__(self, use_llm: bool = True):
        self.blocking_strategy = None
        self.ml_matcher = MLEntityMatcher()
        self.graph = EntityGraph()
        self.probabilistic_linker = ProbabilisticLinker()
        self.llm_resolver = LLMEntityResolver() if use_llm else None
        self.resolved_entities = []

    def resolve_entities(self, entities: List[EntityRecord],
                         blocking_attributes: List[str]) -> List[EntityRecord]:
        # Complete entity resolution pipeline

        print(f"\nResolving {len(entities)} entities...")

        # Step 1: Blocking
        print("Step 1: Blocking for candidate generation")
        self.blocking_strategy = BlockingStrategy(blocking_attributes)
        candidate_pairs = list(
            self.blocking_strategy.get_candidate_pairs(entities))
        print(f"  Generated {len(candidate_pairs)} candidate pairs")

        # Step 2: Matching
        print("Step 2: Calculating match scores")
        matches = []

        for entity1, entity2 in candidate_pairs:
            # ML-based matching
            match = self.ml_matcher.predict_match(entity1, entity2)

            # LLM verification for uncertain matches
            if self.llm_resolver and 0.6 < match.match_score < 0.8:
                llm_match = self.llm_resolver.resolve_with_llm(
                    entity1, entity2)
                if llm_match.match_score > match.match_score:
                    match = llm_match

            if match.is_match:
                matches.append(match)

        print(f"  Found {len(matches)} matching pairs")

        # Step 3: Graph construction
        print("Step 3: Building entity graph")
        for entity in entities:
            self.graph.add_entity(entity)

        for match in matches:
            self.graph.add_match(
                match.entity1.record_id,
                match.entity2.record_id,
                match.match_score
            )

        # Step 4: Clustering
        print("Step 4: Finding entity clusters")
        clusters = self.graph.find_connected_components(threshold=0.7)
        print(f"  Found {len(clusters)} entity clusters")

        # Step 5: Master record creation
        print("Step 5: Creating master records")
        master_records = []

        for cluster in clusters:
            if len(cluster) == 1:
                # Single entity - no merge needed
                entity_id = list(cluster)[0]
                master_records.append(self.graph.nodes[entity_id])
            else:
                # Merge multiple entities
                if self.llm_resolver:
                    cluster_entities = [self.graph.nodes[eid]
                                        for eid in cluster]
                    master = self.llm_resolver.resolve_conflicts(
                        cluster_entities)
                else:
                    master = self.graph.merge_entities(cluster)

                master_records.append(master)

        self.resolved_entities = master_records
        print(f"  Created {len(master_records)} master records")

        return master_records

    def evaluate_resolution(self, true_matches: List[Tuple[str, str]]) -> Dict[str, float]:
        # Evaluate resolution quality

        # Build predicted match set
        predicted_matches = set()

        for cluster in self.graph.clusters:
            cluster_list = list(cluster)
            for i in range(len(cluster_list)):
                for j in range(i + 1, len(cluster_list)):
                    pair = tuple(sorted([cluster_list[i], cluster_list[j]]))
                    predicted_matches.add(pair)

        # Build true match set
        true_matches_set = set(tuple(sorted(pair)) for pair in true_matches)

        # Calculate metrics
        true_positives = len(predicted_matches & true_matches_set)
        false_positives = len(predicted_matches - true_matches_set)
        false_negatives = len(true_matches_set - predicted_matches)

        precision = true_positives / \
            (true_positives + false_positives) if (true_positives +
                                                   false_positives) > 0 else 0
        recall = true_positives / \
            (true_positives + false_negatives) if (true_positives +
                                                   false_negatives) > 0 else 0
        f1 = 2 * precision * recall / \
            (precision + recall) if (precision + recall) > 0 else 0

        return {
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'true_positives': true_positives,
            'false_positives': false_positives,
            'false_negatives': false_negatives
        }

# Example usage functions


def example_basic_matching():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Entity Matching")
    print("="*60)

    # Create sample entities
    entities = [
        EntityRecord("source1", "1", {
            "name": "John Smith",
            "address": "123 Main St",
            "phone": "555-1234"
        }),
        EntityRecord("source2", "2", {
            "name": "Jon Smith",  # Typo
            "address": "123 Main Street",
            "phone": "5551234"
        }),
        EntityRecord("source1", "3", {
            "name": "Jane Doe",
            "address": "456 Oak Ave",
            "phone": "555-5678"
        }),
        EntityRecord("source2", "4", {
            "name": "John Smith",  # Different John Smith
            "address": "789 Elm St",
            "phone": "555-9999"
        })
    ]

    # Test similarity calculator
    calc = SimilarityCalculator()

    print("Similarity scores:")
    print(
        f"  'John Smith' vs 'Jon Smith': {calc.jaro_winkler_similarity('John Smith', 'Jon Smith'):.3f}")
    print(
        f"  '123 Main St' vs '123 Main Street': {calc.token_sort_similarity('123 Main St', '123 Main Street'):.3f}")
    print(
        f"  '555-1234' vs '5551234': {calc.exact_match('5551234', '5551234'):.3f}")

    # Test ML matcher
    matcher = MLEntityMatcher()

    print("\nML matching:")
    match1 = matcher.predict_match(entities[0], entities[1])
    print(
        f"  Entity 1 vs Entity 2: {match1.match_score:.3f} ({match1.match_type.value})")

    match2 = matcher.predict_match(entities[0], entities[3])
    print(
        f"  Entity 1 vs Entity 4: {match2.match_score:.3f} ({match2.match_type.value})")


def example_blocking():
    print("\n" + "="*60)
    print("EXAMPLE 2: Blocking Strategy")
    print("="*60)

    # Create more entities
    entities = []
    for i in range(100):
        entities.append(EntityRecord(f"source{i%3}", f"{i}", {
            "name": f"Person {i//10}",
            "address": f"{i*100} Street {i%10}",
            "phone": f"555-{1000+i}",
            "email": f"person{i}@example.com"
        }))

    # Apply blocking
    blocker = BlockingStrategy(["name", "address"])
    blocks = blocker.create_blocks(entities)

    print(f"Created {len(blocks)} blocks from {len(entities)} entities")

    # Show block sizes
    block_sizes = [len(entities) for entities in blocks.values()]
    print(
        f"Block sizes: min={min(block_sizes)}, max={max(block_sizes)}, avg={sum(block_sizes)/len(block_sizes):.1f}")

    # Count candidate pairs
    total_pairs = sum(n * (n-1) // 2 for n in block_sizes)
    naive_pairs = len(entities) * (len(entities) - 1) // 2

    print(
        f"Candidate pairs: {total_pairs} (vs {naive_pairs} without blocking)")
    print(f"Reduction: {(1 - total_pairs/naive_pairs)*100:.1f}%")


def example_graph_resolution():
    print("\n" + "="*60)
    print("EXAMPLE 3: Graph-based Resolution")
    print("="*60)

    # Create entities with transitive matches
    entities = [
        EntityRecord("s1", "A", {"name": "John Smith",
                     "email": "john@example.com"}),
        EntityRecord("s2", "B", {"name": "J. Smith",
                     "email": "john@example.com"}),
        EntityRecord("s3", "C", {"name": "J. Smith", "phone": "555-1234"}),
        EntityRecord("s4", "D", {"name": "John S.", "phone": "555-1234"}),
        EntityRecord("s5", "E", {"name": "Jane Doe",
                     "email": "jane@example.com"})
    ]

    # Build graph
    graph = EntityGraph()
    for entity in entities:
        graph.add_entity(entity)

    # Add matches
    graph.add_match("A", "B", 0.9)  # Same email
    graph.add_match("B", "C", 0.8)  # Same name
    graph.add_match("C", "D", 0.85)  # Same phone

    # Find clusters
    clusters = graph.find_connected_components(threshold=0.75)

    print(f"Found {len(clusters)} clusters:")
    for i, cluster in enumerate(clusters):
        print(f"  Cluster {i+1}: {cluster}")

    # Create master records
    for cluster in clusters:
        master = graph.merge_entities(cluster)
        if master:
            print(f"\nMaster record for cluster {cluster}:")
            print(f"  {json.dumps(master.attributes, indent=4)}")


def example_probabilistic_linkage():
    print("\n" + "="*60)
    print("EXAMPLE 4: Probabilistic Record Linkage")
    print("="*60)

    # Create training data
    training_pairs = [
        # Matches
        (EntityRecord("s1", "1", {"name": "John Smith", "age": 30}),
         EntityRecord("s2", "2", {"name": "John Smith", "age": 30}),
         True),

        (EntityRecord("s1", "3", {"name": "Jane Doe", "age": 25}),
         EntityRecord("s2", "4", {"name": "Jane Doe", "age": 26}),
         True),

        # Non-matches
        (EntityRecord("s1", "5", {"name": "John Smith", "age": 30}),
         EntityRecord("s2", "6", {"name": "John Smith", "age": 50}),
         False),

        (EntityRecord("s1", "7", {"name": "Bob Jones", "age": 40}),
         EntityRecord("s2", "8", {"name": "Bob Smith", "age": 40}),
         False)
    ]

    # Train probabilistic linker
    linker = ProbabilisticLinker()
    linker.estimate_probabilities(training_pairs)

    print("Estimated probabilities:")
    print("m-probabilities (P(agree|match)):", linker.m_probabilities)
    print("u-probabilities (P(agree|non-match)):", linker.u_probabilities)

    # Test on new pair
    test1 = EntityRecord("s1", "9", {"name": "Alice Brown", "age": 35})
    test2 = EntityRecord("s2", "10", {"name": "Alice Brown", "age": 36})

    weight = linker.calculate_match_weight(test1, test2)
    classification = linker.classify_pair(test1, test2)

    print(f"\nTest pair:")
    print(f"  Weight: {weight:.3f}")
    print(f"  Classification: {classification.value}")


def example_complete_pipeline():
    print("\n" + "="*60)
    print("EXAMPLE 5: Complete Resolution Pipeline")
    print("="*60)

    # Create realistic dataset
    entities = [
        # Duplicate set 1
        EntityRecord("crm", "crm_001", {
            "name": "Acme Corporation",
            "address": "123 Business Park",
            "phone": "555-0100",
            "email": "contact@acme.com",
            "id": "ACM001"
        }),
        EntityRecord("erp", "erp_501", {
            "name": "ACME Corp.",
            "address": "123 Business Park Drive",
            "phone": "5550100",
            "id": "ACME-001"
        }),

        # Duplicate set 2
        EntityRecord("crm", "crm_002", {
            "name": "TechStart Inc",
            "address": "456 Innovation Way",
            "email": "info@techstart.com"
        }),
        EntityRecord("billing", "bill_101", {
            "name": "TechStart Incorporated",
            "address": "456 Innovation Way Suite 200",
            "phone": "555-0200"
        }),

        # Unique entity
        EntityRecord("crm", "crm_003", {
            "name": "Global Systems",
            "address": "789 Enterprise Blvd",
            "email": "contact@globalsys.com"
        })
    ]

    # Run pipeline
    pipeline = EntityResolutionPipeline(
        use_llm=False)  # Set to True if LLM available
    master_records = pipeline.resolve_entities(entities, ["name", "email"])

    print(f"\nResolution complete:")
    print(f"  Input entities: {len(entities)}")
    print(f"  Master records: {len(master_records)}")

    # Show master records
    for i, master in enumerate(master_records):
        print(f"\nMaster Record {i+1}:")
        print(f"  Source: {master.source_id}")
        print(f"  ID: {master.record_id}")
        print(f"  Attributes: {json.dumps(master.attributes, indent=4)}")

    # Evaluate if we have ground truth
    true_matches = [("crm_001", "erp_501"), ("crm_002", "bill_101")]
    metrics = pipeline.evaluate_resolution(true_matches)

    print("\nEvaluation metrics:")
    print(f"  Precision: {metrics['precision']:.3f}")
    print(f"  Recall: {metrics['recall']:.3f}")
    print(f"  F1 Score: {metrics['f1_score']:.3f}")

# Main execution


def main():
    print("="*60)
    print("MODULE 12: ENTITY RESOLUTION")
    print("="*60)
    print("\nIdentifying and merging duplicate entities across sources")

    # Run examples
    example_basic_matching()
    example_blocking()
    example_graph_resolution()
    example_probabilistic_linkage()
    example_complete_pipeline()

    print("\n" + "="*60)
    print("KEY CONCEPTS")
    print("="*60)

    print("\nEntity resolution challenges:")
    print("- Name variations (John Smith, J. Smith, Smith John)")
    print("- Data quality issues (typos, missing values)")
    print("- Scale (quadratic comparison problem)")
    print("- Transitive matches (A=B, B=C, therefore A=C)")

    print("\nResolution techniques:")
    print("- Blocking: Reduce comparisons by 95%+")
    print("- Multiple similarity metrics: Capture different match types")
    print("- Graph clustering: Handle transitive matches")
    print("- Probabilistic linkage: Formal statistical framework")
    print("- LLM enhancement: Complex semantic matching")

    print("\nImplementation considerations:")
    print("- Start with high-precision blocking")
    print("- Use multiple similarity metrics")
    print("- Human validation for uncertain matches")
    print("- Maintain lineage of merged records")
    print("- Monitor for over-merging")


if __name__ == "__main__":
    main()
