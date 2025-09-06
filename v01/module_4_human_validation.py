# module_4_human_validation.py
# Module 4: Human-in-the-Loop Validation
# Interactive validation and feedback for schema mappings

import os
import json
import pickle
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
from dataclasses import dataclass, asdict
from enum import Enum

# Import previous modules
from llm_providers import llm_interface, llm_json_call
from module_3_pattern_matching import PatternMatchingPipeline, SemanticMatcher

# Part 1: Validation Data Structures


class ValidationStatus(Enum):
    # Status of validation for each mapping
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CORRECTED = "corrected"
    NEEDS_REVIEW = "needs_review"


class ConfidenceLevel(Enum):
    # Confidence levels for visual indication
    HIGH = "high"        # > 0.8
    MEDIUM = "medium"    # 0.6 - 0.8
    LOW = "low"          # < 0.6


@dataclass
class MappingValidation:
    # Validation record for a single mapping
    mapping_id: str
    source_field: str
    target_field: str
    confidence: float
    status: ValidationStatus
    reviewer: str
    review_timestamp: Optional[datetime]
    corrected_target: Optional[str]
    reviewer_notes: Optional[str]
    sample_data: Optional[Dict[str, List]]

    def to_dict(self) -> Dict[str, Any]:
        # Convert to dictionary for serialization
        result = asdict(self)
        result['status'] = self.status.value
        if self.review_timestamp:
            result['review_timestamp'] = self.review_timestamp.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        # Create from dictionary
        data['status'] = ValidationStatus(data['status'])
        if data.get('review_timestamp'):
            data['review_timestamp'] = datetime.fromisoformat(
                data['review_timestamp'])
        return cls(**data)

    def get_confidence_level(self) -> ConfidenceLevel:
        # Determine confidence level for display
        if self.confidence > 0.8:
            return ConfidenceLevel.HIGH
        elif self.confidence >= 0.6:
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW

# Part 2: Validation Session Manager


class ValidationSession:
    # Manages a validation session for schema mappings

    def __init__(self, session_id: str = None):
        self.session_id = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.validations = {}  # mapping_id -> MappingValidation
        self.metadata = {
            'created_at': datetime.now().isoformat(),
            'total_mappings': 0,
            'reviewed': 0,
            'approved': 0,
            'rejected': 0,
            'corrected': 0
        }
        self.feedback_log = []

    def add_mapping_for_validation(self, source_field: str, target_field: str,
                                   confidence: float, source_samples: List = None,
                                   target_samples: List = None) -> str:
        # Add a mapping for validation

        mapping_id = f"{source_field}_{target_field}_{len(self.validations)}"

        validation = MappingValidation(
            mapping_id=mapping_id,
            source_field=source_field,
            target_field=target_field,
            confidence=confidence,
            status=ValidationStatus.PENDING,
            reviewer=None,
            review_timestamp=None,
            corrected_target=None,
            reviewer_notes=None,
            sample_data={
                'source': source_samples[:5] if source_samples else [],
                'target': target_samples[:5] if target_samples else []
            }
        )

        self.validations[mapping_id] = validation
        self.metadata['total_mappings'] += 1

        return mapping_id

    def get_next_for_review(self, confidence_threshold: float = 0.8) -> Optional[MappingValidation]:
        # Get next mapping that needs review
        # Prioritize low confidence mappings

        pending = [v for v in self.validations.values()
                   if v.status == ValidationStatus.PENDING]

        if not pending:
            return None

        # Sort by confidence (lowest first)
        pending.sort(key=lambda x: x.confidence)

        # Return lowest confidence or first below threshold
        for validation in pending:
            if validation.confidence < confidence_threshold:
                return validation

        return pending[0] if pending else None

    def submit_validation(self, mapping_id: str, status: ValidationStatus,
                          reviewer: str, corrected_target: str = None,
                          notes: str = None) -> bool:
        # Submit validation for a mapping

        if mapping_id not in self.validations:
            return False

        validation = self.validations[mapping_id]
        validation.status = status
        validation.reviewer = reviewer
        validation.review_timestamp = datetime.now()
        validation.corrected_target = corrected_target
        validation.reviewer_notes = notes

        # Update metadata
        self.metadata['reviewed'] += 1

        if status == ValidationStatus.APPROVED:
            self.metadata['approved'] += 1
        elif status == ValidationStatus.REJECTED:
            self.metadata['rejected'] += 1
        elif status == ValidationStatus.CORRECTED:
            self.metadata['corrected'] += 1

        # Log feedback
        self.feedback_log.append({
            'timestamp': datetime.now().isoformat(),
            'mapping_id': mapping_id,
            'action': status.value,
            'reviewer': reviewer
        })

        return True

    def get_validation_summary(self) -> Dict[str, Any]:
        # Get summary of validation session

        total = self.metadata['total_mappings']
        reviewed = self.metadata['reviewed']

        summary = {
            'session_id': self.session_id,
            'progress': {
                'total': total,
                'reviewed': reviewed,
                'remaining': total - reviewed,
                'percentage': (reviewed / total * 100) if total > 0 else 0
            },
            'decisions': {
                'approved': self.metadata['approved'],
                'rejected': self.metadata['rejected'],
                'corrected': self.metadata['corrected']
            },
            'confidence_distribution': self.get_confidence_distribution()
        }

        return summary

    def get_confidence_distribution(self) -> Dict[str, int]:
        # Get distribution of mappings by confidence level

        distribution = {
            'high': 0,
            'medium': 0,
            'low': 0
        }

        for validation in self.validations.values():
            level = validation.get_confidence_level()
            distribution[level.value] += 1

        return distribution

    def save_session(self, filepath: str = None) -> str:
        # Save validation session to file

        if not filepath:
            filepath = f"validation_sessions/session_{self.session_id}.json"

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        session_data = {
            'session_id': self.session_id,
            'metadata': self.metadata,
            'validations': {
                k: v.to_dict() for k, v in self.validations.items()
            },
            'feedback_log': self.feedback_log
        }

        with open(filepath, 'w') as f:
            json.dump(session_data, f, indent=2)

        return filepath

    @classmethod
    def load_session(cls, filepath: str):
        # Load validation session from file

        with open(filepath, 'r') as f:
            session_data = json.load(f)

        session = cls(session_data['session_id'])
        session.metadata = session_data['metadata']
        session.feedback_log = session_data['feedback_log']

        # Reconstruct validations
        for mapping_id, val_data in session_data['validations'].items():
            session.validations[mapping_id] = MappingValidation.from_dict(
                val_data)

        return session

# Part 3: Feedback Learning System


class FeedbackLearner:
    # Learn from human feedback to improve future mappings

    def __init__(self):
        self.correction_patterns = {}  # source -> corrected mappings
        self.rejection_patterns = {}   # patterns that are commonly rejected
        self.approval_patterns = {}    # patterns that are commonly approved
        self.learning_history = []

    def learn_from_validation(self, validation: MappingValidation):
        # Learn from a single validation

        source = validation.source_field.lower()
        target = validation.target_field.lower()

        if validation.status == ValidationStatus.APPROVED:
            # Learn approval pattern
            if source not in self.approval_patterns:
                self.approval_patterns[source] = {}
            self.approval_patterns[source][target] = \
                self.approval_patterns[source].get(target, 0) + 1

        elif validation.status == ValidationStatus.REJECTED:
            # Learn rejection pattern
            if source not in self.rejection_patterns:
                self.rejection_patterns[source] = {}
            self.rejection_patterns[source][target] = \
                self.rejection_patterns[source].get(target, 0) + 1

        elif validation.status == ValidationStatus.CORRECTED:
            # Learn correction pattern
            if source not in self.correction_patterns:
                self.correction_patterns[source] = {}
            corrected = validation.corrected_target.lower()
            self.correction_patterns[source][corrected] = \
                self.correction_patterns[source].get(corrected, 0) + 1

        # Log learning event
        self.learning_history.append({
            'timestamp': datetime.now().isoformat(),
            'source': source,
            'target': target,
            'status': validation.status.value,
            'corrected': validation.corrected_target
        })

    def suggest_mapping(self, source_field: str, candidate_targets: List[str]) -> Tuple[str, float]:
        # Suggest best mapping based on learned patterns

        source_lower = source_field.lower()

        # Check if we have correction history for this source
        if source_lower in self.correction_patterns:
            # Find most common correction
            corrections = self.correction_patterns[source_lower]
            if corrections:
                best_correction = max(corrections, key=corrections.get)
                # High confidence if we've seen this correction multiple times
                confidence = min(
                    0.9, 0.6 + (corrections[best_correction] * 0.1))
                return best_correction, confidence

        # Check approval patterns
        if source_lower in self.approval_patterns:
            approvals = self.approval_patterns[source_lower]
            # Find approved target that matches our candidates
            for candidate in candidate_targets:
                if candidate.lower() in approvals:
                    confidence = min(
                        0.85, 0.5 + (approvals[candidate.lower()] * 0.1))
                    return candidate, confidence

        # No learned pattern found
        return None, 0.0

    def get_learning_stats(self) -> Dict[str, Any]:
        # Get statistics about learned patterns

        total_corrections = sum(
            len(patterns) for patterns in self.correction_patterns.values()
        )
        total_approvals = sum(
            sum(patterns.values()) for patterns in self.approval_patterns.values()
        )
        total_rejections = sum(
            sum(patterns.values()) for patterns in self.rejection_patterns.values()
        )

        return {
            'total_feedback_events': len(self.learning_history),
            'unique_corrections': total_corrections,
            'total_approvals': total_approvals,
            'total_rejections': total_rejections,
            'fields_with_corrections': len(self.correction_patterns),
            'fields_with_approvals': len(self.approval_patterns),
            'fields_with_rejections': len(self.rejection_patterns)
        }

    def save_learning(self, filepath: str = "feedback_learning.json"):
        # Save learned patterns

        data = {
            'correction_patterns': self.correction_patterns,
            'rejection_patterns': self.rejection_patterns,
            'approval_patterns': self.approval_patterns,
            # Keep last 1000 events
            'learning_history': self.learning_history[-1000:],
            'stats': self.get_learning_stats()
        }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

    def load_learning(self, filepath: str = "feedback_learning.json") -> bool:
        # Load learned patterns

        if not os.path.exists(filepath):
            return False

        with open(filepath, 'r') as f:
            data = json.load(f)

        self.correction_patterns = data.get('correction_patterns', {})
        self.rejection_patterns = data.get('rejection_patterns', {})
        self.approval_patterns = data.get('approval_patterns', {})
        self.learning_history = data.get('learning_history', [])

        return True

# Part 4: Interactive Validation Interface


class ValidationInterface:
    # Command-line interface for validation (Streamlit alternative for workshop)

    def __init__(self, session: ValidationSession, learner: FeedbackLearner = None):
        self.session = session
        self.learner = learner or FeedbackLearner()
        self.current_validation = None
        self.reviewer_name = "default_reviewer"

    def display_mapping(self, validation: MappingValidation):
        # Display a mapping for review

        print("\n" + "="*60)
        print("MAPPING FOR REVIEW")
        print("="*60)

        confidence_level = validation.get_confidence_level()

        print(f"\nSource Field: {validation.source_field}")
        print(f"Target Field: {validation.target_field}")
        print(
            f"Confidence: {validation.confidence:.2f} ({confidence_level.value.upper()})")

        if validation.sample_data:
            print("\nSample Data:")
            if validation.sample_data.get('source'):
                print(
                    f"  Source samples: {validation.sample_data['source'][:3]}")
            if validation.sample_data.get('target'):
                print(
                    f"  Target samples: {validation.sample_data['target'][:3]}")

        # Show learned suggestions if available
        if self.learner:
            suggestion, conf = self.learner.suggest_mapping(
                validation.source_field,
                [validation.target_field]
            )
            if suggestion:
                print(
                    f"\nLearned suggestion: {suggestion} (confidence: {conf:.2f})")

    def get_user_decision(self) -> Tuple[ValidationStatus, Optional[str], Optional[str]]:
        # Get validation decision from user

        print("\nValidation Options:")
        print("  1. APPROVE - Mapping is correct")
        print("  2. REJECT - Mapping is incorrect")
        print("  3. CORRECT - Provide correct mapping")
        print("  4. SKIP - Need more information")
        print("  5. EXIT - Save and exit validation")

        while True:
            choice = input("\nYour decision (1-5): ").strip()

            if choice == '1':
                notes = input("Notes (optional): ").strip() or None
                return ValidationStatus.APPROVED, None, notes

            elif choice == '2':
                notes = input("Reason for rejection: ").strip() or None
                return ValidationStatus.REJECTED, None, notes

            elif choice == '3':
                corrected = input("Correct target field: ").strip()
                notes = input("Notes (optional): ").strip() or None
                return ValidationStatus.CORRECTED, corrected, notes

            elif choice == '4':
                return ValidationStatus.NEEDS_REVIEW, None, "Skipped for later review"

            elif choice == '5':
                return None, None, None

            else:
                print("Invalid choice. Please enter 1-5.")

    def run_validation_session(self, max_reviews: int = None):
        # Run interactive validation session

        print("\n" + "="*60)
        print("VALIDATION SESSION")
        print("="*60)

        # Get reviewer name
        self.reviewer_name = input("\nEnter your name: ").strip() or "reviewer"

        # Show session summary
        summary = self.session.get_validation_summary()
        print(f"\nSession: {summary['session_id']}")
        print(f"Total mappings: {summary['progress']['total']}")
        print(f"Already reviewed: {summary['progress']['reviewed']}")
        print(f"Remaining: {summary['progress']['remaining']}")

        reviews_done = 0

        while True:
            # Check if we've hit the review limit
            if max_reviews and reviews_done >= max_reviews:
                print(f"\nReached review limit of {max_reviews}")
                break

            # Get next mapping for review
            validation = self.session.get_next_for_review()

            if not validation:
                print("\nNo more mappings to review!")
                break

            # Display mapping
            self.display_mapping(validation)

            # Get decision
            status, corrected_target, notes = self.get_user_decision()

            if status is None:
                # User wants to exit
                print("\nExiting validation session...")
                break

            # Submit validation
            self.session.submit_validation(
                validation.mapping_id,
                status,
                self.reviewer_name,
                corrected_target,
                notes
            )

            # Learn from feedback
            if status != ValidationStatus.NEEDS_REVIEW:
                self.learner.learn_from_validation(validation)
                reviews_done += 1

                print(
                    f"\nValidation recorded. ({reviews_done} reviews completed)")

        # Save session and learning
        session_file = self.session.save_session()
        self.learner.save_learning()

        print(f"\nSession saved to: {session_file}")
        print("Learning patterns saved.")

        # Show final summary
        self.show_final_summary()

    def show_final_summary(self):
        # Show summary of validation session

        summary = self.session.get_validation_summary()

        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)

        print(f"\nProgress: {summary['progress']['reviewed']}/{summary['progress']['total']} " +
              f"({summary['progress']['percentage']:.1f}%)")

        print("\nDecisions:")
        for decision, count in summary['decisions'].items():
            print(f"  {decision.capitalize()}: {count}")

        print("\nConfidence Distribution:")
        for level, count in summary['confidence_distribution'].items():
            print(f"  {level.capitalize()}: {count}")

        # Show learning stats
        if self.learner:
            stats = self.learner.get_learning_stats()
            print("\nLearning Statistics:")
            print(f"  Total feedback events: {stats['total_feedback_events']}")
            print(
                f"  Unique corrections learned: {stats['unique_corrections']}")

# Part 5: Validation Pipeline Integration


class ValidationPipeline:
    # Complete pipeline with validation

    def __init__(self, use_ai: bool = True):
        self.pattern_pipeline = PatternMatchingPipeline(use_ai=use_ai)
        self.session = None
        self.learner = FeedbackLearner()
        self.interface = None

    def prepare_validation_session(self, file_paths: List[str]) -> ValidationSession:
        # Prepare mappings for validation

        print("\nPreparing validation session...")

        # Analyze sources and create mappings
        schemas, data_frames = self.pattern_pipeline.analyze_sources(
            file_paths)
        alignment_results = self.pattern_pipeline.align_schemas(
            schemas, data_frames)

        # Create validation session
        session = ValidationSession()

        # Add mappings to session
        for mapping in alignment_results['mappings']:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            confidence = mapping['confidence']

            # Get sample data
            source_samples = []
            target_samples = []

            # Find source and target data
            for source_name, df in data_frames.items():
                if source_field in df.columns:
                    source_samples = df[source_field].dropna().head(5).tolist()
                if target_field in df.columns:
                    target_samples = df[target_field].dropna().head(5).tolist()

            session.add_mapping_for_validation(
                source_field, target_field, confidence,
                source_samples, target_samples
            )

        self.session = session
        return session

    def run_interactive_validation(self, max_reviews: int = None):
        # Run interactive validation

        if not self.session:
            print("No validation session prepared!")
            return

        # Load previous learning if exists
        self.learner.load_learning()

        # Create interface
        self.interface = ValidationInterface(self.session, self.learner)

        # Run validation
        self.interface.run_validation_session(max_reviews)

    def apply_validated_mappings(self, session: ValidationSession) -> Dict[str, Any]:
        # Apply validated mappings to create final schema

        approved_mappings = []
        corrected_mappings = []

        for validation in session.validations.values():
            if validation.status == ValidationStatus.APPROVED:
                approved_mappings.append({
                    'source': validation.source_field,
                    'target': validation.target_field,
                    'confidence': validation.confidence
                })
            elif validation.status == ValidationStatus.CORRECTED:
                corrected_mappings.append({
                    'source': validation.source_field,
                    'original_target': validation.target_field,
                    'corrected_target': validation.corrected_target,
                    'confidence': 1.0  # Human validated
                })

        return {
            'approved_mappings': approved_mappings,
            'corrected_mappings': corrected_mappings,
            'total_validated': len(approved_mappings) + len(corrected_mappings)
        }

# Example functions


def example_basic_validation():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Validation Workflow")
    print("="*60)

    # Create a validation session with mock data
    session = ValidationSession()

    # Add some mappings for validation
    mappings = [
        ('Customer_ID', 'customer_id', 0.95),
        ('CustID', 'customer_id', 0.85),
        ('date', 'created_date', 0.65),  # Ambiguous
        ('amt', 'amount', 0.55),  # Low confidence
        ('status', 'order_status', 0.75)
    ]

    for source, target, conf in mappings:
        session.add_mapping_for_validation(
            source, target, conf,
            source_samples=['sample1', 'sample2'],
            target_samples=['target1', 'target2']
        )

    # Show session summary
    summary = session.get_validation_summary()
    print(f"\nCreated session with {summary['progress']['total']} mappings")
    print(f"Confidence distribution: {summary['confidence_distribution']}")

    # Simulate validation
    print("\nSimulating validation decisions...")

    # Approve high confidence
    session.submit_validation(
        'Customer_ID_customer_id_0',
        ValidationStatus.APPROVED,
        'example_reviewer'
    )

    # Correct low confidence
    session.submit_validation(
        'amt_amount_3',
        ValidationStatus.CORRECTED,
        'example_reviewer',
        corrected_target='total_amount',
        notes='amt refers to total amount in this context'
    )

    # Show updated summary
    summary = session.get_validation_summary()
    print(f"\nAfter validation:")
    print(
        f"  Reviewed: {summary['progress']['reviewed']}/{summary['progress']['total']}")
    print(f"  Approved: {summary['decisions']['approved']}")
    print(f"  Corrected: {summary['decisions']['corrected']}")


def example_feedback_learning():
    print("\n" + "="*60)
    print("EXAMPLE 2: Feedback Learning")
    print("="*60)

    learner = FeedbackLearner()

    # Simulate learning from multiple validations
    validations = [
        MappingValidation(
            mapping_id='1',
            source_field='cust_id',
            target_field='customer_id',
            confidence=0.8,
            status=ValidationStatus.APPROVED,
            reviewer='user1',
            review_timestamp=datetime.now(),
            corrected_target=None,
            reviewer_notes=None,
            sample_data=None
        ),
        MappingValidation(
            mapping_id='2',
            source_field='cust_id',
            target_field='client_id',
            confidence=0.6,
            status=ValidationStatus.REJECTED,
            reviewer='user1',
            review_timestamp=datetime.now(),
            corrected_target=None,
            reviewer_notes='Wrong mapping',
            sample_data=None
        ),
        MappingValidation(
            mapping_id='3',
            source_field='amt',
            target_field='amount',
            confidence=0.5,
            status=ValidationStatus.CORRECTED,
            reviewer='user1',
            review_timestamp=datetime.now(),
            corrected_target='total_amount',
            reviewer_notes=None,
            sample_data=None
        )
    ]

    # Learn from validations
    for validation in validations:
        learner.learn_from_validation(validation)

    # Show learning stats
    stats = learner.get_learning_stats()
    print("\nLearning Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Test suggestions
    print("\nTesting learned suggestions:")

    # Should suggest approved mapping
    suggestion, conf = learner.suggest_mapping(
        'cust_id', ['customer_id', 'client_id'])
    print(f"  cust_id -> {suggestion} (confidence: {conf:.2f})")

    # Should suggest correction
    suggestion, conf = learner.suggest_mapping(
        'amt', ['amount', 'total_amount'])
    print(f"  amt -> {suggestion} (confidence: {conf:.2f})")


def example_validation_with_real_data():
    print("\n" + "="*60)
    print("EXAMPLE 3: Validation with Real Data")
    print("="*60)

    # Check if data exists
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv'
    ]

    existing_files = [f for f in files if os.path.exists(f)]
    if len(existing_files) < 2:
        print("  Need customer data files. Run fake_data_generator.py first.")
        return

    # Create validation pipeline
    pipeline = ValidationPipeline(use_ai=True)

    # Prepare session
    session = pipeline.prepare_validation_session(existing_files[:2])

    # Show what needs validation
    summary = session.get_validation_summary()
    print(f"\nPrepared {summary['progress']['total']} mappings for validation")

    # Show confidence distribution
    print("\nMappings by confidence level:")
    for level, count in summary['confidence_distribution'].items():
        print(f"  {level}: {count}")

    # Get a few examples
    print("\nExample mappings needing validation:")
    for i in range(min(3, len(session.validations))):
        validation = session.get_next_for_review()
        if validation:
            print(f"  {validation.source_field:20} -> {validation.target_field:20} " +
                  f"(conf: {validation.confidence:.2f})")
            # Mark as pending again for actual validation
            validation.status = ValidationStatus.PENDING


def example_batch_validation():
    print("\n" + "="*60)
    print("EXAMPLE 4: Batch Validation with Rules")
    print("="*60)

    # Create session with mappings
    session = ValidationSession()

    # Add various confidence mappings
    test_mappings = [
        ('customer_id', 'customer_id', 1.0),  # Exact match
        ('Customer_ID', 'customer_id', 0.95),  # High confidence
        ('cust_id', 'customer_id', 0.85),
        ('order_date', 'date', 0.65),  # Medium confidence
        ('ship_date', 'date', 0.62),
        ('total', 'amount', 0.45),  # Low confidence
        ('notes', 'description', 0.40)
    ]

    for source, target, conf in test_mappings:
        session.add_mapping_for_validation(source, target, conf)

    print(f"\nCreated {len(test_mappings)} mappings for validation")

    # Apply automatic validation rules
    auto_approved = 0
    needs_review = 0

    for validation in session.validations.values():
        # Auto-approve exact matches
        if validation.source_field.lower() == validation.target_field.lower():
            session.submit_validation(
                validation.mapping_id,
                ValidationStatus.APPROVED,
                'auto_validator',
                notes='Exact match - auto approved'
            )
            auto_approved += 1

        # Auto-approve high confidence
        elif validation.confidence > 0.9:
            session.submit_validation(
                validation.mapping_id,
                ValidationStatus.APPROVED,
                'auto_validator',
                notes='High confidence - auto approved'
            )
            auto_approved += 1

        # Flag low confidence for review
        elif validation.confidence < 0.5:
            validation.status = ValidationStatus.NEEDS_REVIEW
            needs_review += 1

    print(f"\nBatch validation results:")
    print(f"  Auto-approved: {auto_approved}")
    print(f"  Needs review: {needs_review}")
    print(
        f"  Pending manual validation: {len(test_mappings) - auto_approved - needs_review}")


def example_export_validated_schema():
    print("\n" + "="*60)
    print("EXAMPLE 5: Export Validated Schema")
    print("="*60)

    # Create and populate a session
    session = ValidationSession("example_export")

    # Add validated mappings
    mappings = [
        ('Customer_ID', 'customer_id', 0.95, ValidationStatus.APPROVED),
        ('Full_Name', 'customer_name', 0.85, ValidationStatus.APPROVED),
        ('amt', 'amount', 0.55, ValidationStatus.CORRECTED, 'total_amount'),
        ('notes', 'description', 0.40, ValidationStatus.REJECTED),
    ]

    for source, target, conf, status, *corrected in mappings:
        mapping_id = session.add_mapping_for_validation(source, target, conf)

        corrected_target = corrected[0] if corrected else None
        session.submit_validation(
            mapping_id, status, 'reviewer',
            corrected_target=corrected_target
        )

    # Apply validated mappings
    pipeline = ValidationPipeline()
    results = pipeline.apply_validated_mappings(session)

    print(f"\nValidation Results:")
    print(f"  Total validated: {results['total_validated']}")
    print(f"  Approved: {len(results['approved_mappings'])}")
    print(f"  Corrected: {len(results['corrected_mappings'])}")

    # Export to file
    export_path = 'validation_sessions/validated_schema.json'
    os.makedirs(os.path.dirname(export_path), exist_ok=True)

    with open(export_path, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nExported to: {export_path}")

    # Show final mappings
    print("\nFinal validated mappings:")
    for mapping in results['approved_mappings']:
        print(f"  {mapping['source']:20} -> {mapping['target']}")

    for mapping in results['corrected_mappings']:
        print(
            f"  {mapping['source']:20} -> {mapping['corrected_target']} (corrected)")

# Main execution


def main():
    print("="*60)
    print("MODULE 4: HUMAN-IN-THE-LOOP VALIDATION")
    print("="*60)
    print("\nInteractive validation and feedback system")

    # Run examples
    example_basic_validation()
    example_feedback_learning()
    example_validation_with_real_data()
    example_batch_validation()
    example_export_validated_schema()

    # Offer interactive validation
    print("\n" + "="*60)
    print("INTERACTIVE VALIDATION")
    print("="*60)

    response = input(
        "\nWould you like to run an interactive validation session? (y/n): ")

    if response.lower() == 'y':
        files = [
            'workshop_data/customers_v1.csv',
            'workshop_data/customers_v2.csv',
            'workshop_data/customers_v3.csv'
        ]

        existing = [f for f in files if os.path.exists(f)]

        if len(existing) >= 2:
            pipeline = ValidationPipeline(use_ai=True)
            pipeline.prepare_validation_session(existing[:2])
            pipeline.run_interactive_validation(
                max_reviews=5)  # Limit for demo
        else:
            print("Insufficient data files for validation demo.")

    print("\n" + "="*60)
    print("MODULE 4 COMPLETE")
    print("="*60)
    print("\nCapabilities demonstrated:")
    print("- Validation data structures and session management")
    print("- Interactive validation interface")
    print("- Learning from human feedback")
    print("- Batch validation with rules")
    print("- Confidence-based prioritization")
    print("- Export of validated schemas")
    print("\nNext: Module 5 - Integration - Unified SQL Interface")


if __name__ == "__main__":
    main()
